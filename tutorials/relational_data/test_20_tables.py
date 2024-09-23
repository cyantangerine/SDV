import json
import sqlite3

import pandas as pd
import tqdm


def fetch_data_from_sqlite(path='./data_sqlite.db'):
    conn = sqlite3.connect(path)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(query, conn)
    table_names = tables['name'].tolist()
    tables_dict = {}
    metadata = {
        "tables": {}
    }

    # Define relationships manually as they are not in the database
    relationships = {
        "Assignment": {
            "course_id": ("Course", "course_id")
        },
        "Enrollment": {"student_id": ("Student", "student_id"),
                       "course_id": ("Course", "course_id")},
        "Submission": {"assignment_id": ("Assignment", "assignment_id"),
                       "student_id": ("Student", "student_id")},
        "Schedule": {"course_id": ("Course", "course_id"),
                     "professor_id": ("Professor", "professor_id")},
        "Major": {"department_id": ("Department", "department_id")},
        "CourseTextbook": {"course_id": ("Course", "course_id"),
                           "textbook_id": ("Textbook", "textbook_id")},
        "Book": {"library_id": ("Library", "library_id")},
        "BookLoan": {"book_id": ("Book", "book_id"), "student_id": ("Student", "student_id")},
        "ResearchProject": {"group_id": ("ResearchGroup", "group_id")},
        "ProjectMember": {"project_id": ("ResearchProject", "project_id"),
                          "professor_id": ("Professor", "professor_id")},
        "LabEquipment": {"lab_id": ("Lab", "lab_id")},
        "EquipmentMaintenance": {"equipment_id": ("LabEquipment", "equipment_id")}
    }

    for table_name in table_names:
        table_data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        tables_dict[table_name] = table_data
        schema_query = f"PRAGMA table_info({table_name})"
        schema_info = pd.read_sql_query(schema_query, conn)
        primary_key = schema_info[schema_info['pk'] == 1]['name'].values[0]
        fields_metadata = {}

        for _, row in schema_info.iterrows():
            field_name = row['name']
            field_type = 'id' if 'id' in field_name else 'categorical'
            field_details = {
                "type": field_type
            }
            if field_type == 'id':
                if True:  # field_name != primary_key:
                    ref_info = relationships.get(table_name, {}).get(field_name, None)
                    if ref_info:
                        field_details['ref'] = {
                            "field": ref_info[1],
                            "table": ref_info[0]
                        }
            fields_metadata[field_name] = field_details

        metadata['tables'][table_name] = {
            "primary_key": primary_key,
            "fields": fields_metadata
        }

    # metadata_json = json.dumps(metadata, indent=4)
    conn.close()
    return metadata, tables_dict


def save_tables(tables, ename='output.xlsx'):
    with pd.ExcelWriter(ename) as writer:
        for name, table in tqdm.tqdm(tables.items()):
            table.to_excel(writer, sheet_name=name, index=False)


if __name__ == '__main__':
    # Call the function and save the metadata
    metadata, tables = fetch_data_from_sqlite()
    with open('./db_metadata.json', 'w') as f:
        f.write(metadata)
