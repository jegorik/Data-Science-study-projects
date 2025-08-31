# HR Data Analysis Project 📊

A comprehensive data analysis tool for HR decision-making that processes employee data from multiple office locations to generate insights on employee performance, satisfaction, and departmental metrics.

## 🎯 Project Overview

This project analyzes HR data from two office locations (A and B) combined with HR records to provide actionable insights for human resources management. The analysis covers employee retention, performance metrics, departmental efficiency, and promotion patterns.

## ✨ Features

- **Automated Data Pipeline**: Downloads and processes XML data files automatically
- **Multi-Office Analysis**: Combines data from multiple office locations
- **Employee Metrics**: Comprehensive employee performance and retention analysis
- **Departmental Insights**: Salary and workload analysis by department
- **Promotion Analysis**: Evaluation of promotion patterns and their impact
- **Specific Queries**: Targeted analysis for business-critical questions
- **Robust Error Handling**: Professional-grade error management and logging
- **Type Safety**: Full type annotations for maintainable code

## 📋 Tasks Implemented

### Task 1: Data Loading and Indexing
- Loads XML data from multiple sources
- Reindexes dataframes with proper employee IDs (A/B prefixes)
- Validates data integrity

### Task 2: Data Merging
- Merges office data with HR records
- Creates unified dataset for analysis
- Handles data conflicts and missing records

### Task 3: Employee Metrics Aggregation
- Analyzes metrics by employee departure status
- Calculates project workload statistics
- Evaluates company tenure and accident rates
- Provides performance evaluation insights

### Task 4: Pivot Table Analysis
- Department-wise salary and workload analysis
- Promotion impact on employee satisfaction
- Time-based performance trends

### Task 5: Business Insights
- Top-performing departments identification
- IT department project analysis
- Individual employee performance tracking

## 🛠️ Technologies Used

- **Python 3.8+**
- **pandas**: Data manipulation and analysis
- **requests**: HTTP library for data downloading
- **typing**: Type annotations for code safety
- **logging**: Comprehensive logging system
- **pathlib**: Modern path handling

## 📁 Project Structure

```
HR Data Analysis/
├── README.md                          # This file
├── explore.py                         # Main analysis script
```

## 🚀 Getting Started

### Prerequisites

Ensure you have Python 3.8+ installed with the following packages:

```bash
pip install pandas requests
```

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd "HR Data Analysis"
```

2. Run the analysis:
```bash
python explore.py
```

The script will automatically:
- Create the necessary data directory
- Download required XML files if missing
- Process and analyze the data
- Display comprehensive results

## 💡 Usage Examples

### Basic Analysis
```python
from explore import DataAnalysis

# Initialize the analysis
analyzer = DataAnalysis('../Data/', ['A_office_data.xml', 'B_office_data.xml', 'hr_data.xml'])

# Get unified dataset
dataset = analyzer.project_dataframes['unified_dataset']

# Generate specific insights
top_departments = analyzer.top_10_departments_by_hours(dataset)
it_projects = analyzer.total_projects_it_low_salary(dataset)
```

### Comprehensive Report
```python
# Generate complete analysis report
report = analyzer.generate_comprehensive_report()
print(report)
```

The analysis provides insights such as:

### Employee Metrics by Departure Status
```python
{
    'number_project': {'median': {0: 3.0, 1: 5.0}, 'count_bigger_5': {0: 120, 1: 85}},
    'time_spend_company': {'mean': {0: 2.40, 1: 4.85}, 'median': {0: 3.0, 1: 4.0}},
    'Work_accident': {'mean': {0: 0.24, 1: 0.11}},
    'last_evaluation': {'mean': {0: 0.64, 1: 0.77}, 'std': {0: 0.25, 1: 0.13}}
}
```

### Department Analysis
- Identifies departments with unusual salary-hour relationships
- Highlights promotion pattern anomalies
- Provides workload distribution insights

## 🔍 Key Insights Generated

1. **Employee Retention Patterns**: Analysis of factors contributing to employee turnover
2. **Department Efficiency**: Workload vs. compensation analysis by department
3. **Promotion Effectiveness**: Impact of promotions on employee satisfaction and performance
4. **Resource Allocation**: Identification of high-performing departments and employees

## 🧪 Data Sources

The project processes three main data sources:

- **A_office_data.xml**: Employee data from Office A (projects, hours, tenure, etc.)
- **B_office_data.xml**: Employee data from Office B (same structure as Office A)
- **hr_data.xml**: HR records (satisfaction, evaluations, departure status)

### Data Schema

#### Office Data (A & B)
- `number_project`: Number of projects worked on
- `average_monthly_hours`: Monthly workload in hours
- `time_spend_company`: Years with the company
- `Work_accident`: Work injury history
- `promotion_last_5years`: Recent promotion status
- `Department`: Employee department
- `salary`: Salary level (low/medium/high)
- `employee_office_id`: Office-specific employee ID

#### HR Data
- `satisfaction_level`: Job satisfaction score
- `last_evaluation`: Most recent performance evaluation
- `left`: Employee departure status
- `employee_id`: Company-wide employee ID

## 🛡️ Error Handling

The project includes comprehensive error handling:

- **Network Issues**: Robust download with retry logic
- **Data Validation**: Schema validation before processing
- **Missing Data**: Graceful handling of incomplete records
- **Type Safety**: Runtime type checking for data integrity

## 📈 Performance Considerations

- **Memory Efficient**: Optimized DataFrame operations
- **Scalable Design**: Modular architecture for easy extension
- **Logging**: Comprehensive logging for debugging and monitoring
- **Caching**: Intelligent data caching to avoid redundant operations
