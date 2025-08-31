import pandas as pd
import requests
import os
import logging
from typing import Dict, List, Any, Optional, Union, Hashable
from pathlib import Path


class DataAnalysis:
    """
    A comprehensive HR data analysis class that handles XML data loading,
    processing, and generates insights for HR decision-making.

    This class processes employee data from multiple office locations and
    HR records to provide metrics on employee performance, satisfaction,
    and departmental analysis.
    """

    def __init__(self, directory: str, required_files: List[str]) -> None:
        """
        Initialize the DataAnalysis class.

        Args:
            directory: Path to the data directory
            required_files: List of required XML files to process
        """
        self.setup_logging()
        self.project_files: Dict[str, Any] = {
            'directory': directory,
            'required_files': required_files,
        }

        self.project_dataframes: Dict[str, Optional[pd.DataFrame]] = {
            'a_office_data': None,
            'b_office_data': None,
            'hr_data': None,
            'unified_dataset': None
        }

        self.prepare_project_structure()

    def setup_logging(self) -> None:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def prepare_project_structure(self) -> None:
        """Prepare the complete project structure by executing all necessary steps."""
        try:
            self.check_directory(self.project_files['directory'])
            self.check_necessary_files(self.project_files['directory'], self.project_files['required_files'])
            self.create_dataframes(self.project_files['directory'], self.project_files['required_files'])
            self.reindex_dataframes()
            unified_dataset = self.generate_unified_dataset()
            self.project_dataframes['unified_dataset'] = unified_dataset
            self.logger.info("Data preparation completed successfully")
        except Exception as e:
            self.logger.error(f"Error in data preparation: {e}")
            raise

    def check_directory(self, directory: str) -> str:
        """
        Check if directory exists, create if not.

        Args:
            directory: Path to check/create

        Returns:
            Status message
        """
        directory_path = Path(directory)
        if not directory_path.exists():
            directory_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Directory created: {directory}")
            return 'Directory created.'
        else:
            self.logger.info(f"Directory already exists: {directory}")
            return 'Directory already exists.'

    def check_necessary_files(self, directory: str, required_files: List[str]) -> str:
        """
        Check if required files exist, download if missing.

        Args:
            directory: Directory to check for files
            required_files: List of required file names

        Returns:
            Status message
        """
        all_exist = all(os.path.exists(os.path.join(directory, f)) for f in required_files)

        if not all_exist:
            self.logger.info("Missing files detected, starting download...")
            try:
                self._download_data_files(directory)
                return 'Files downloaded successfully'
            except Exception as e:
                self.logger.error(f"Error downloading files: {e}")
                return f'Download failed: {e}'
        else:
            self.logger.info("All required files exist")
            return 'All files exist'

    def _download_data_files(self, directory: str) -> None:
        """Download data files from remote sources."""
        file_urls = {
            'A_office_data.xml': "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1",
            'B_office_data.xml': "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1",
            'hr_data.xml': "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        }

        for filename, url in file_urls.items():
            self.logger.info(f"Downloading {filename}")
            try:
                response = requests.get(url, allow_redirects=True, timeout=30)
                response.raise_for_status()

                file_path = os.path.join(directory, filename)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                self.logger.info(f"Successfully downloaded {filename}")

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Failed to download {filename}: {e}")
                raise

    def create_dataframes(self, directory: str, required_files: List[str]) -> str:
        """
        Create pandas DataFrames from XML files.

        Args:
            directory: Directory containing the files
            required_files: List of file names to process

        Returns:
            Status message
        """
        try:
            file_mapping = {
                'a_office_data': required_files[0],
                'b_office_data': required_files[1],
                'hr_data': required_files[2]
            }

            for key, filename in file_mapping.items():
                file_path = os.path.join(directory, filename)
                self.project_dataframes[key] = pd.read_xml(file_path)
                self.logger.info(f"Loaded {key} with shape {self.project_dataframes[key].shape}")

            return 'DataFrames created successfully.'

        except Exception as e:
            self.logger.error(f"Error creating DataFrames: {e}")
            raise

    def reindex_dataframes(self) -> None:
        """Reindex all dataframes with proper employee IDs."""
        try:
            # Reindex A and B office data with proper prefixes
            self.project_dataframes['a_office_data'].index = [
                f'A{x}' for x in self.project_dataframes['a_office_data']['employee_office_id']
            ]
            self.project_dataframes['b_office_data'].index = [
                f'B{x}' for x in self.project_dataframes['b_office_data']['employee_office_id']
            ]

            # Set HR data index
            self.project_dataframes['hr_data'].set_index('employee_id', inplace=True)

            self.logger.info("DataFrames reindexed successfully")

        except Exception as e:
            self.logger.error(f"Error reindexing DataFrames: {e}")
            raise

    def print_dataframe_data(self, dataset_name: str) -> None:
        """
        Print index and column information for a dataset.

        Args:
            dataset_name: Name of the dataset to print info for
        """
        if dataset_name not in self.project_dataframes:
            self.logger.warning(f"Dataset {dataset_name} not found")
            return

        df = self.project_dataframes[dataset_name]
        if df is not None:
            print(df.index.tolist())
            print(df.columns.tolist())
        else:
            self.logger.warning(f"Dataset {dataset_name} is None")

    def generate_unified_dataset(self) -> pd.DataFrame:
        """
        Generate unified dataset by combining office data with HR data.

        Returns:
            Unified pandas DataFrame
        """
        try:
            # Combine office data
            office_dfs = [
                self.project_dataframes['a_office_data'],
                self.project_dataframes['b_office_data']
            ]
            unified_dataset = pd.concat(office_dfs, ignore_index=False)

            # Merge with HR data
            unified_dataset = unified_dataset.merge(
                self.project_dataframes['hr_data'],
                left_index=True,
                right_index=True,
                how='inner'
            )

            # Clean up unnecessary columns
            columns_to_drop = ['employee_office_id', 'employee_id']
            unified_dataset = unified_dataset.drop(
                columns=[col for col in columns_to_drop if col in unified_dataset.columns]
            )

            # Sort by index
            unified_dataset = unified_dataset.sort_index()

            self.logger.info(f"Unified dataset created with shape {unified_dataset.shape}")
            return unified_dataset

        except Exception as e:
            self.logger.error(f"Error generating unified dataset: {e}")
            raise

    def validate_dataset(self, dataset: pd.DataFrame) -> bool:
        """
        Validate that the dataset contains required columns.

        Args:
            dataset: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        required_columns = [
            'number_project', 'average_monthly_hours', 'time_spend_company',
            'Work_accident', 'promotion_last_5years', 'Department', 'salary',
            'satisfaction_level', 'last_evaluation', 'left'
        ]

        missing_columns = [col for col in required_columns if col not in dataset.columns]

        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        return True

    # Function that counts the number of employees who worked on more than five projects.
    def count_bigger_5(self, series: pd.Series) -> int:
        """Count employees who worked on more than 5 projects."""
        return (series > 5).sum()

    # Task: The HR boss asks for the following metrics:
    # - The median number of projects the employees in a group worked on, and how many employees worked on more than five projects;
    # - The mean and median time spent in the company;
    # - The share of employees who've had work accidents;
    # - The mean and standard deviation of the last evaluation score.
    def employees_metrics(self, dataset: pd.DataFrame) -> Dict[Hashable, Any]:
        """
        Generate employee metrics grouped by departure status.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            Dictionary with aggregated metrics
        """
        if not self.validate_dataset(dataset):
            raise ValueError("Dataset validation failed")

        try:
            result_df = dataset.groupby(['left']).agg({
                'number_project': ['median', self.count_bigger_5],
                'time_spend_company': ['mean', 'median'],
                'Work_accident': 'mean',
                'last_evaluation': ['mean', 'std']
            }).round(2)

            return result_df.to_dict()

        except Exception as e:
            self.logger.error(f"Error calculating employee metrics: {e}")
            raise

    # Use df.pivot_table() to generate the first pivot table: Department as index, left and salary as columns, average_monthly_hours as values.
    # Store median values in the table.
    def median_salaries_by_department(self, dataset: pd.DataFrame) -> Dict[Hashable, Any]:
        """
        Generate pivot table of median salaries by department with filtering.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            Dictionary with filtered pivot table data
        """
        try:
            result = dataset.pivot_table(
                index='Department',
                columns=['left', 'salary'],
                values='average_monthly_hours',
                aggfunc='median'
            ).round(2)

            # Apply filtering conditions
            condition1 = result[(0, 'high')] < result[(0, 'medium')]
            condition2 = result[(1, 'low')] < result[(1, 'high')]

            filtered_result = result.loc[condition1 | condition2]
            return filtered_result.to_dict()

        except Exception as e:
            self.logger.error(f"Error calculating department salaries: {e}")
            raise

    # Use df.pivot_table() to generate the second pivot table: time_spend_company as index, promotion_last_5years as column, satisfaction_level and last_evaluation as values. Store the min, max, and mean values in the table.
    def employees_time_spend_company(self, dataset: pd.DataFrame) -> Dict[Hashable, Any]:
        """
        Analyze employee satisfaction and evaluation by time and promotion status.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            Dictionary with filtered time analysis data
        """
        try:
            result = dataset.pivot_table(
                index='time_spend_company',
                columns='promotion_last_5years',
                values=['satisfaction_level', 'last_evaluation'],
                aggfunc=['min', 'max', 'mean']
            ).round(2)

            # Filter where non-promoted have higher evaluation than promoted
            condition = result['mean', 'last_evaluation', 0] > result['mean', 'last_evaluation', 1]
            filtered_result = result.loc[condition]

            return filtered_result.to_dict()

        except Exception as e:
            self.logger.error(f"Error calculating time spend analysis: {e}")
            raise

    # Task 5: Get the insights - Answer specific questions about the data
    def top_10_departments_by_hours(self, dataset: pd.DataFrame) -> List[str]:
        """
        Find departments of the top 10 employees by working hours.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            List of department names
        """
        try:
            top_10 = dataset.nlargest(10, 'average_monthly_hours')
            return top_10['Department'].tolist()
        except Exception as e:
            self.logger.error(f"Error finding top departments: {e}")
            raise

    def total_projects_it_low_salary(self, dataset: pd.DataFrame) -> int:
        """
        Calculate total projects for IT department employees with low salaries.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            Total number of projects
        """
        try:
            filtered_data = dataset[
                (dataset['Department'] == 'IT') &
                (dataset['salary'] == 'low')
            ]
            return int(filtered_data['number_project'].sum())
        except Exception as e:
            self.logger.error(f"Error calculating IT projects: {e}")
            raise

    def specific_employees_data(self, dataset: pd.DataFrame) -> List[List[Union[float, None]]]:
        """
        Get evaluation scores and satisfaction levels for specific employees.

        Args:
            dataset: DataFrame containing employee data

        Returns:
            List of [evaluation_score, satisfaction_level] for each employee
        """
        employee_ids = ['A4', 'B7064', 'A3033']
        result = []

        try:
            for emp_id in employee_ids:
                if emp_id in dataset.index:
                    emp_data = dataset.loc[emp_id, ['last_evaluation', 'satisfaction_level']]
                    result.append([
                        float(emp_data['last_evaluation']),
                        float(emp_data['satisfaction_level'])
                    ])
                else:
                    self.logger.warning(f"Employee {emp_id} not found in dataset")
                    result.append([None, None])

            return result

        except Exception as e:
            self.logger.error(f"Error getting specific employee data: {e}")
            raise

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive report with all analyses.

        Returns:
            Dictionary containing all analysis results
        """
        dataset = self.project_dataframes.get('unified_dataset')
        if dataset is None:
            raise ValueError("Unified dataset not available")

        report = {
            'dataset_info': {
                'shape': dataset.shape,
                'columns': dataset.columns.tolist(),
                'index_sample': dataset.index.tolist()[:10]
            },
            'employee_metrics': self.employees_metrics(dataset),
            'department_analysis': self.median_salaries_by_department(dataset),
            'time_analysis': self.employees_time_spend_company(dataset),
            'insights': {
                'top_departments': self.top_10_departments_by_hours(dataset),
                'it_projects_total': self.total_projects_it_low_salary(dataset),
                'specific_employees': self.specific_employees_data(dataset)
            }
        }

        return report


def main() -> None:
    """Main function to run the HR data analysis."""
    # Configure pandas display options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    try:
        # Initialize data analysis
        data_analysis = DataAnalysis(
            '../Data/',
            ['A_office_data.xml', 'B_office_data.xml', 'hr_data.xml']
        )

        dataset = data_analysis.project_dataframes.get('unified_dataset')
        if dataset is None:
            raise ValueError("Failed to create unified dataset")

        # Task outputs as specified
        print("=== HR Data Analysis Results ===\n")

        # Task 1: Load data and modify indexes
        print("A office indexes:")
        print(data_analysis.project_dataframes['a_office_data'].index.tolist())
        print("B office indexes:")
        print(data_analysis.project_dataframes['b_office_data'].index.tolist())
        print("HR data indexes:")
        print(data_analysis.project_dataframes['hr_data'].index.tolist())

        # Task 2: Merge everything
        print("\nFinal dataset indexes:")
        print(dataset.index.tolist())
        print("Final dataset columns:")
        print(dataset.columns.tolist())

        # Task 3: Aggregate the data
        print("\nEmployee metrics:")
        print(data_analysis.employees_metrics(dataset))

        # Task 4: Draw up pivot tables
        print("\nMedian salaries by department:")
        print(data_analysis.median_salaries_by_department(dataset))
        print("Employees time spend company:")
        print(data_analysis.employees_time_spend_company(dataset))

        # Task 5: Get the insights
        print("\nTop 10 departments by hours:")
        print(data_analysis.top_10_departments_by_hours(dataset))
        print("Total IT low salary projects:")
        print(data_analysis.total_projects_it_low_salary(dataset))
        print("Specific employees data:")
        print(data_analysis.specific_employees_data(dataset))

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise


if __name__ == '__main__':
    main()
