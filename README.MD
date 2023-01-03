This is a tool that pulls CubeCobra's S3 bucket, and uploads the bulk exports to DropBox.  
The tool is designed to not do anything if there's nothing to update.  
The uploaded files on Dropbox are in 7z format to save space.  
After the job runs, an email is sent to the inbox of your choice about the results.  
  
No credentials are included, you'll have to get those from Dekkaru over at CubeCobra and your own Dropbox account, respectively.  

How to get Dropbox refresh code:
https://www.dropbox.com/oauth2/authorize?client_id=<APP_KEY>&token_access_type=offline&response_type=code