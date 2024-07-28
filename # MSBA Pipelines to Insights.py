# MSBA Pipelines to Insights
# Team Members: Soryn Lurding, Olabisi Odusanya, Tim Jones
# Dependencies:
# pip install xlsxwriter
# pip install openpyxl

import pandas as pd
import re
from openpyxl import Workbook

class CSVProcessor:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.data = self.read_csv_file()
        if self.data is not None:
            self.rename_columns()
            self.convert_to_text_and_datetime()  # Updated method call
            self.remove_invalid_linkedin_urls()
            self.recode_birth_months()
            self.recode_experience_levels()
            self.recode_class_attendance()
            self.recode_job_status()
            self.add_unique_identifier()
            self.format_emails_and_linkedin()
            self.filter_data()

    def read_csv_file(self):
        try:
            data = pd.read_csv(self.csv_file, dtype=str)
            # Filter out rows where StudentId is missing
            data = data[data['StudentID'].notna()]
            return data
        except FileNotFoundError:
            print(f"Error: The file {self.csv_file} was not found.")
        except pd.errors.EmptyDataError:
            print("Error: The file is empty.")
        except pd.errors.ParserError:
            print("Error: The file could not be parsed.")
        return None

    def rename_columns(self):
        self.data.rename(columns={
            'Timestamp': 'TimeStamp',
            'Username': 'Email',
            'StudentID': 'StudentId',
            'Job Status': 'JobStatus',
            'Birth Month': 'BirthMonth',
            'Which class session will you attend? (Select all that apply)': 'ClassAttendance',
            'Programming Experience level (Any language)': 'ProgrammingExperience',
            'Python Programming Experience level': 'PythonProgrammingExperience',
            'LinkedIn Profile URL': 'LinkedinUrl'
        }, inplace=True)

    def convert_to_text_and_datetime(self):
        # Convert all columns to text
        self.data = self.data.astype(str)
        
        # Clean the TimeStamp column to remove anything besides date and time
        self.data['TimeStamp'] = self.data['TimeStamp'].apply(lambda x: re.sub(r'[^0-9-: ]', '', x))
        
        # Convert the cleaned TimeStamp column to datetime
        self.data['TimeStamp'] = pd.to_datetime(self.data['TimeStamp'], errors='coerce')

    def remove_invalid_linkedin_urls(self):
        self.data = self.data[self.data['LinkedinUrl'].str.contains("linkedin.com/in/")]

    def recode_birth_months(self):
        month_map = {
            "January": "1", "February": "2", "March": "3", "April": "4",
            "May": "5", "June": "6", "July": "7", "August": "8",
            "September": "9", "October": "10", "November": "11", "December": "12"
        }
        self.data['BirthMonth'] = self.data['BirthMonth'].map(month_map).fillna(self.data['BirthMonth'])

    def recode_experience_levels(self):
        experience_map = {
            "Zero Experience": "0",
            "Beginner": "1",
            "Capable": "2",
            "Intermediate": "3",
            "Effective": "4",
            "Experienced": "5",
            "Advance": "6",
            "Distinguished": "7",
            "Master": "9"
        }
        self.data['ProgrammingExperience'] = self.data['ProgrammingExperience'].map(experience_map).fillna(self.data['ProgrammingExperience'])
        self.data['PythonProgrammingExperience'] = self.data['PythonProgrammingExperience'].map(experience_map).fillna(self.data['PythonProgrammingExperience'])

    def recode_class_attendance(self):
        def count_days(attendance):
            days = ["Day 1", "Day 2", "Day 3", "Day 4", "Day 5"]
            return sum(attendance.count(day) for day in days)
        
        self.data['ClassAttendance'] = self.data['ClassAttendance'].apply(lambda x: count_days(x) if pd.notna(x) else 0)

    def recode_job_status(self):
        job_status_map = {
            "Working in Data": "1",
            "Seeking Job in Data": "0"
        }
        self.data['JobStatus'] = self.data['JobStatus'].map(job_status_map).fillna(self.data['JobStatus'])

    def add_unique_identifier(self):
        self.data['TimeStamp'] = pd.to_datetime(self.data['TimeStamp'])
        sorted_data = self.data.sort_values(by='TimeStamp', ascending=False)
        unique_data = sorted_data.loc[sorted_data.duplicated(subset='StudentId', keep='first') == False]
        self.data = unique_data.reset_index(drop=True)
        self.data['UniqueId'] = range(10001, 10001 + len(self.data))

    def format_emails_and_linkedin(self):
        self.data['Email'] = self.data['Email'].apply(self.validate_email)
        self.data['LinkedinUrl'] = self.data['LinkedinUrl'].apply(self.format_linkedin_url)

    def validate_email(self, email):
        try:
            if "@" in email and "." in email.split("@")[1]:
                return email
        except Exception as e:
            print(f"Error validating email {email}: {e}")
        return ""

    def format_linkedin_url(self, url):
        try:
            if url.startswith("www"):
                url = "https://" + url
            if "linkedin.com/in/" not in url:
                if "linkedin.com" in url:
                    return "https://www.linkedin.com/in/" + url.split("linkedin.com")[-1].strip("/").split("/")[-1]
                else:
                    return "https://www.linkedin.com/in/" + url.strip("/")
            return url
        except Exception as e:
            print(f"Error formatting LinkedIn URL {url}: {e}")
            return ""

    def filter_data(self):
        self.data = self.data[
            (self.data['JobStatus'] == "0") &
            (self.data['PythonProgrammingExperience'].astype(int) >= 3) &
            (self.data['ProgrammingExperience'].astype(int) >= 3) &
            (self.data['ClassAttendance'].astype(int) >= 3)
        ]

    def extract_student_data(self):
        return self.data[['UniqueId', 'StudentId', 'Email', 'BirthMonth', 'LinkedinUrl', 'JobStatus']]

    def extract_experience_data(self):
        return self.data[['UniqueId', 'ProgrammingExperience', 'PythonProgrammingExperience']]

    def extract_class_attendance_data(self):
        return self.data[['UniqueId', 'ClassAttendance']]

    def export_to_excel(self, student_data, experience_data, class_attendance_data, output_file):
        try:
            with pd.ExcelWriter(output_file) as writer:
                student_data.to_excel(writer, sheet_name='Student', index=False)
                experience_data.to_excel(writer, sheet_name='Experience', index=False)
                class_attendance_data.to_excel(writer, sheet_name='ClassAttendance', index=False)
            print(f"Data has been successfully exported to {output_file}")
        except Exception as e:
            print(f"Error exporting data to Excel: {e}")

def main():
    csv_file = 'C:/Users/tjone/Desktop/UofL/Pipelines to Insights/Pipeline Class Registration.csv'
    output_file = 'C:/Users/tjone/Desktop/UofL/Pipelines to Insights/Processed_Class_Registration.xlsx'

    processor = CSVProcessor(csv_file)
    
    if processor.data is not None:
        student_data = processor.extract_student_data()
        experience_data = processor.extract_experience_data()
        class_attendance_data = processor.extract_class_attendance_data()

        processor.export_to_excel(student_data, experience_data, class_attendance_data, output_file)

if __name__ == "__main__":
    main()

