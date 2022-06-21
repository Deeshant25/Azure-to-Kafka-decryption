# Integration2
An app to download a PGP encrypted file from Azure Blob Storage, decrypt it and write its contents to a file

Steps to run:

1) Run npm install.
2) Run 'azurite --silent --loose --location ~/.azurite' in a terminal, and keep this terminal running.
3) Use Azure Storage Explorer to create a Container named 'files' in the emulator and upload the encrypted file to the container (in case different names are chosen for the container and/or the encrypted file, make the same changes in app.js).
4) Paste the Connection String from the emulator in the AZURE_STORAGE_CONNECTION_STRING variable in app.js.
5) Run app.js.
