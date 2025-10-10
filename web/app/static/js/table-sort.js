/**
 * Table Sorting Utility
 *
 * Provides client-side sorting for statistics tables with the following features:
 * - Click column headers to sort ascending/descending
 * - Visual sort indicators (↑/↓ arrows)
 * - Numeric vs alphabetic sorting based on data type
 * - Keeps career totals row pinned at bottom
 * - Preserves sticky column functionality
 */

class TableSorter {
    constructor(tableId, options = {}) {
        this.table = document.getElementById(tableId);
        if (!this.table) {
            console.error(`Table with ID "${tableId}" not found`);
            return;
        }

        this.tbody = this.table.querySelector('tbody');
        this.headers = this.table.querySelectorAll('thead th');
        this.currentSort = {
            column: options.defaultColumn || 0,
            direction: options.defaultDirection || 'desc'
        };

        this.init();
    }

    init() {
        // Add click handlers to sortable headers
        this.headers.forEach((header, index) => {
            // Skip Team and League columns (indices may vary, check by header text)
            const headerText = header.textContent.trim();
            if (headerText === 'Team' || headerText === 'Lg') {
                return;
            }

            // Make header clickable
            header.style.cursor = 'pointer';
            header.classList.add('hover:bg-gray-200', 'transition-colors');

            // Add click handler
            header.addEventListener('click', () => this.sortByColumn(index));

            // Add sort indicator to first column (default sort)
            if (index === this.currentSort.column) {
                this.updateSortIndicator(header, this.currentSort.direction);
            }
        });
    }

    sortByColumn(columnIndex) {
        // Toggle direction if same column, otherwise default to descending
        if (columnIndex === this.currentSort.column) {
            this.currentSort.direction = this.currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
            this.currentSort.column = columnIndex;
            this.currentSort.direction = 'desc';
        }

        // Remove all existing sort indicators
        this.headers.forEach(header => {
            const indicator = header.querySelector('.sort-indicator');
            if (indicator) {
                indicator.remove();
            }
        });

        // Add indicator to current column
        this.updateSortIndicator(this.headers[columnIndex], this.currentSort.direction);

        // Perform the sort
        this.sortRows(columnIndex, this.currentSort.direction);
    }

    updateSortIndicator(header, direction) {
        const indicator = document.createElement('span');
        indicator.className = 'sort-indicator ml-1';
        indicator.textContent = direction === 'asc' ? '↑' : '↓';
        header.appendChild(indicator);
    }

    sortRows(columnIndex, direction) {
        // Get all rows except career totals
        const rows = Array.from(this.tbody.querySelectorAll('tr'));

        // Separate career totals row (has bold class or specific styling)
        let careerRow = null;
        const dataRows = rows.filter(row => {
            if (row.classList.contains('bg-gray-200') ||
                row.classList.contains('border-t-2') ||
                row.querySelector('td')?.textContent.trim() === 'Career') {
                careerRow = row;
                return false;
            }
            return true;
        });

        // Sort data rows
        dataRows.sort((rowA, rowB) => {
            const cellA = rowA.cells[columnIndex];
            const cellB = rowB.cells[columnIndex];

            if (!cellA || !cellB) return 0;

            // Get cell values (innerText to handle links)
            let valueA = cellA.innerText.trim();
            let valueB = cellB.innerText.trim();

            // Handle missing values
            if (valueA === '-') valueA = null;
            if (valueB === '-') valueB = null;

            // Handle null values (sort to end)
            if (valueA === null && valueB === null) return 0;
            if (valueA === null) return 1;
            if (valueB === null) return -1;

            // Determine if numeric
            const isNumeric = this.isNumericValue(valueA) && this.isNumericValue(valueB);

            let comparison = 0;
            if (isNumeric) {
                // Numeric comparison
                const numA = this.parseNumericValue(valueA);
                const numB = this.parseNumericValue(valueB);
                comparison = numA - numB;
            } else {
                // Alphabetic comparison
                comparison = valueA.localeCompare(valueB);
            }

            return direction === 'asc' ? comparison : -comparison;
        });

        // Clear tbody
        while (this.tbody.firstChild) {
            this.tbody.removeChild(this.tbody.firstChild);
        }

        // Re-add sorted rows
        dataRows.forEach((row, index) => {
            // Alternate row colors
            if (index % 2 === 0) {
                row.classList.remove('bg-gray-50');
                row.classList.add('bg-white');
            } else {
                row.classList.remove('bg-white');
                row.classList.add('bg-gray-50');
            }
            this.tbody.appendChild(row);
        });

        // Add career totals row at the end
        if (careerRow) {
            this.tbody.appendChild(careerRow);
        }
    }

    isNumericValue(value) {
        // Check if value looks like a number
        // Handles: integers, decimals, percentages, negative numbers
        return /^-?\d+\.?\d*$/.test(value) || /^\.\d+$/.test(value);
    }

    parseNumericValue(value) {
        // Remove leading decimal point if present (.262 -> 0.262)
        if (value.startsWith('.')) {
            value = '0' + value;
        }
        return parseFloat(value);
    }
}

// Export for use in templates
window.TableSorter = TableSorter;
