from utils import update_dbf_record  # <- Import the function

from win10toast import ToastNotifier

file_path = "C:/UBSACC2015/Sample/arcust.dbf"  # ✅ No `def` here

update_dbf_record(
    file_path=file_path,
    key_field="NAME",
    key_value="SANTRONIC COMPUTER",
    target_field="NAME2",
    new_value="UPDATE SAMPLE data"
)

toast = ToastNotifier()

toast.show_toast(
    "3 data got conflict",
    "system and mobile both changed on Amount",
    duration = 20,
    icon_path = "icon.ico",
)