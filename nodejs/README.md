Rempah app

Database Selector > Select which UBS DIRECTORY For Accounting & Inventory 

- Accounting Path: C:\UBSACC2015\Company_Data_Folder\
- Inventory & Billing Path: C:\UBSSTK2015\Company_Data_Folder\

Customer
- arcust.dbf (UBSACC2015) (UBSSTK2015)
- icarea.dbf (UBSACC2015) (UBSSTK2015)

Supplier
- apvend.dbf (UBSACC2015) (UBSSTK2015)
- icarea.dbf (UBSACC2015) (UBSSTK2015)

Item
- Icitem.dbf

Transaction
- ictran.dbf
- artran.dbf

Payment
- arpay.dbf
- arpost.dbf
- gldata.dbf
- glbatch.dbf
- glpost.dbf

also have icagent.dbf for rempah, cause they want to seperate based on agent as well and as the area an location, these have its own dbf 
Connect to Online DATABASE FOR APP IOS / ANDROID 

You will need to make sure the database  has the same value for both accounting folder and inventory folder. Cause they have the same name dbf, but in different folders. 

To restart the demo, 

Uninstall the Access UBS program -> use ccleaner to clean the windows application cache, and windows cache as well, then go to hidden file -> proceed to find program data -> delete the folder call SAGE