"""
Ollama Client Module

API client for Ollama with error handling, retry logic, and model management.
Provides interface for generating newspaper articles using local LLMs.

Features:
- Configurable endpoint and model selection
- Exponential backoff retry logic for network failures
- Model availability checking
- Benchmarking capabilities for model selection
- Timeout management
"""

import time
import requests
from typing import Dict, Optional, Tuple
from loguru import logger


class OllamaClient:
    """Client for interacting with Ollama API."""

    def __init__(
        self,
        base_url: str = 'http://localhost:11434',
        default_model: str = 'qwen2.5:7b',
        timeout: int = 120
    ):
        """
        Initialize Ollama client.

        Args:
            base_url: Ollama API endpoint (default: http://localhost:11434)
            default_model: Default model to use if not specified
            timeout: Request timeout in seconds (default: 120)
        """
        self.base_url = base_url.rstrip('/')
        self.default_model = default_model
        self.timeout = timeout

        logger.info(f"Initialized OllamaClient: {self.base_url}, default model: {self.default_model}")

    def generate_article(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 400
    ) -> str:
        """
        Generate article text using Ollama.

        Args:
            prompt: Full prompt string
            model: Model name (uses default if not specified)
            temperature: Sampling temperature 0.0-1.0 (default: 0.7)
            max_tokens: Maximum tokens to generate (default: 400 for ~250 words)

        Returns:
            Generated text

        Raises:
            requests.exceptions.RequestException: On network errors
            ValueError: On invalid response format
        """
        model = model or self.default_model

        logger.info(f"Generating article with model: {model}, temp: {temperature}")
        logger.debug(f"Prompt length: {len(prompt)} characters")

        # Prepare request
        endpoint = f"{self.base_url}/api/generate"
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,  # Get complete response
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            }
        }

        # Make request
        start_time = time.time()

        try:
            response = requests.post(
                endpoint,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            # Parse response
            result = response.json()
            generated_text = result.get('response', '')

            elapsed = time.time() - start_time
            logger.info(f"Generation completed in {elapsed:.2f}s, {len(generated_text)} characters")

            return generated_text

        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.timeout}s")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except (KeyError, ValueError) as e:
            logger.error(f"Invalid response format: {e}")
            raise ValueError(f"Could not parse Ollama response: {e}")

    def generate_with_retry(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 400,
        max_retries: int = 3,
        backoff: float = 2.0
    ) -> Tuple[str, Dict]:
        """
        Generate article with exponential backoff retry logic.

        Args:
            prompt: Full prompt string
            model: Model name (uses default if not specified)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            max_retries: Maximum retry attempts (default: 3)
            backoff: Backoff multiplier (default: 2.0)

        Returns:
            Tuple of (generated_text, metadata_dict)
            metadata_dict includes: attempts, total_time, model_used

        Raises:
            Exception: If all retries fail
        """
        model = model or self.default_model
        attempt = 0
        total_start = time.time()
        last_error = None

        while attempt < max_retries:
            attempt += 1
            wait_time = backoff ** (attempt - 1) if attempt > 1 else 0

            if wait_time > 0:
                logger.info(f"Retry attempt {attempt}/{max_retries} after {wait_time:.1f}s wait...")
                time.sleep(wait_time)
            else:
                logger.info(f"Attempt {attempt}/{max_retries}")

            try:
                generated_text = self.generate_article(
                    prompt=prompt,
                    model=model,
                    temperature=temperature,
                    max_tokens=max_tokens
                )

                # Success!
                total_time = time.time() - total_start
                metadata = {
                    'attempts': attempt,
                    'total_time': total_time,
                    'model_used': model,
                    'temperature': temperature,
                    'success': True
                }

                logger.info(f"Generation succeeded on attempt {attempt}, total time: {total_time:.2f}s")
                return generated_text, metadata

            except requests.exceptions.Timeout as e:
                last_error = e
                logger.warning(f"Timeout on attempt {attempt}")

            except requests.exceptions.ConnectionError as e:
                last_error = e
                logger.warning(f"Connection error on attempt {attempt}: {e}")

            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"Request error on attempt {attempt}: {e}")

            except ValueError as e:
                # Parse error - don't retry, these won't fix themselves
                logger.error(f"Parse error (not retrying): {e}")
                raise

        # All retries failed
        total_time = time.time() - total_start
        metadata = {
            'attempts': attempt,
            'total_time': total_time,
            'model_used': model,
            'temperature': temperature,
            'success': False,
            'error': str(last_error)
        }

        logger.error(f"All {max_retries} attempts failed after {total_time:.2f}s")
        raise Exception(f"Article generation failed after {max_retries} attempts: {last_error}")

    def check_model_availability(self, model_name: str) -> bool:
        """
        Check if a model is available (pulled and ready to use).

        Args:
            model_name: Model name to check (e.g., 'qwen2.5:7b')

        Returns:
            True if model is available, False otherwise
        """
        try:
            endpoint = f"{self.base_url}/api/tags"
            response = requests.get(endpoint, timeout=10)
            response.raise_for_status()

            data = response.json()
            models = data.get('models', [])

            # Check if model_name is in the list
            available_models = [m.get('name') for m in models]
            is_available = model_name in available_models

            if is_available:
                logger.info(f"Model '{model_name}' is available")
            else:
                logger.warning(f"Model '{model_name}' not found. Available models: {available_models}")

            return is_available

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to check model availability: {e}")
            return False

    def list_available_models(self) -> list:
        """
        List all available models.

        Returns:
            List of model name strings
        """
        try:
            endpoint = f"{self.base_url}/api/tags"
            response = requests.get(endpoint, timeout=10)
            response.raise_for_status()

            data = response.json()
            models = data.get('models', [])
            model_names = [m.get('name') for m in models]

            logger.info(f"Found {len(model_names)} available models")
            return model_names

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list models: {e}")
            return []

    def benchmark_model(
        self,
        model_name: str,
        test_prompt: str,
        iterations: int = 3
    ) -> Dict:
        """
        Benchmark a model's generation time and output quality.

        Args:
            model_name: Model to test
            test_prompt: Standard test prompt
            iterations: Number of test runs (default: 3)

        Returns:
            Dict with benchmark results:
                - model: Model name
                - iterations: Number of runs
                - avg_time: Average generation time
                - min_time: Minimum time
                - max_time: Maximum time
                - avg_length: Average output length (characters)
                - sample_output: One sample output for quality review
        """
        logger.info(f"Benchmarking model '{model_name}' with {iterations} iterations...")

        if not self.check_model_availability(model_name):
            logger.error(f"Model '{model_name}' not available for benchmarking")
            return {
                'model': model_name,
                'error': 'Model not available'
            }

        times = []
        lengths = []
        sample_output = None

        for i in range(iterations):
            logger.info(f"  Iteration {i+1}/{iterations}")

            try:
                start = time.time()
                output = self.generate_article(
                    prompt=test_prompt,
                    model=model_name,
                    temperature=0.7,
                    max_tokens=400
                )
                elapsed = time.time() - start

                times.append(elapsed)
                lengths.append(len(output))

                if i == 0:
                    sample_output = output

            except Exception as e:
                logger.error(f"  Iteration {i+1} failed: {e}")

        if not times:
            return {
                'model': model_name,
                'error': 'All iterations failed'
            }

        results = {
            'model': model_name,
            'iterations': len(times),
            'avg_time': sum(times) / len(times),
            'min_time': min(times),
            'max_time': max(times),
            'avg_length': sum(lengths) / len(lengths) if lengths else 0,
            'sample_output': sample_output[:500] if sample_output else None  # First 500 chars
        }

        logger.info(f"Benchmark complete for '{model_name}':")
        logger.info(f"  Avg time: {results['avg_time']:.2f}s")
        logger.info(f"  Min/Max: {results['min_time']:.2f}s / {results['max_time']:.2f}s")
        logger.info(f"  Avg length: {results['avg_length']:.0f} characters")

        return results

    def health_check(self) -> bool:
        """
        Check if Ollama service is running and accessible.

        Returns:
            True if service is healthy, False otherwise
        """
        try:
            endpoint = f"{self.base_url}/api/tags"
            response = requests.get(endpoint, timeout=5)
            response.raise_for_status()

            logger.info("Ollama service health check: OK")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Ollama service health check failed: {e}")
            return False


def get_fallback_model(preferred_model: str, client: OllamaClient) -> str:
    """
    Get a fallback model if preferred model is unavailable.

    Fallback hierarchy:
    - qwen2.5:14b -> qwen2.5:7b -> qwen2.5:3b
    - llama3.1:70b -> llama3.1:8b

    Args:
        preferred_model: Desired model name
        client: OllamaClient instance

    Returns:
        Available model name (may be preferred or fallback)
    """
    # Define fallback chains
    fallback_chains = {
        'qwen2.5:14b': ['qwen2.5:7b', 'qwen2.5:3b', 'llama3.1:8b'],
        'qwen2.5:7b': ['qwen2.5:3b', 'llama3.1:8b'],
        'qwen2.5:3b': ['llama3.1:8b'],
        'llama3.1:70b': ['llama3.1:8b', 'qwen2.5:7b'],
        'llama3.1:8b': ['qwen2.5:7b'],
    }

    # Check if preferred model is available
    if client.check_model_availability(preferred_model):
        return preferred_model

    # Try fallbacks
    logger.warning(f"Preferred model '{preferred_model}' not available, trying fallbacks...")

    fallbacks = fallback_chains.get(preferred_model, [])
    for fallback in fallbacks:
        if client.check_model_availability(fallback):
            logger.info(f"Using fallback model: {fallback}")
            return fallback

    # No fallback worked, return preferred anyway (will fail with clear error)
    logger.error(f"No fallback models available for '{preferred_model}'")
    return preferred_model