import dbf





def update_dbf_record(file_path, key_field, key_value, target_field, new_value):
    """
    Updates a field in a DBF file where a specific field matches a given value.

    :param file_path: Full path to the .dbf file
    :param key_field: Field to match on (e.g., 'name')
    :param key_value: Value to match (e.g., 'Alice')
    :param target_field: Field to update (e.g., 'balance')
    :param new_value: New value to set
    """
    table = dbf.Table(file_path)
    table.open(mode=dbf.READ_WRITE)

    updated = False
    for record in table:
        if str(record[key_field]).strip() == str(key_value).strip():
            with record:
                record[target_field] = new_value
            print(f"Record updated: {record}")
            updated = True

    table.close()

    if not updated:
        print("No matching record found.")



file_path = "C:/UBSACC2015/Sample/arcust.dbf"  # ✅ No `def` here

update_dbf_record(
    file_path=file_path,
    key_field="NAME",
    key_value="SANTRONIC COMPUTER",
    target_field="NAME2",
    new_value="SANTRONIC TECH"
)