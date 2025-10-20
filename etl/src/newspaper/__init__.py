"""
Newspaper Article Generation Module

This module handles automated generation of newspaper-style articles
for Branch family member performances using LLM (Ollama) integration.

Modules:
    branch_detector: Identifies games featuring Branch family members
    newsworthiness: Scores games for article generation priority
    game_context: Fetches complete game metadata and player stats
    game_log_parser: Extracts play-by-play details from game_logs.csv
    prompt_builder: Constructs LLM prompts for article generation
    ollama_client: API client for Ollama LLM service
    article_processor: Parses LLM output and stores articles
    pipeline: Orchestrates the end-to-end article generation workflow
"""

__version__ = "1.0.0"