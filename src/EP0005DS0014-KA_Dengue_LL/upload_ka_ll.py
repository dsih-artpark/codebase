import epipipeline
from epipipeline.upload.ka_dengue_upload import (unzip_folder, rename_ka_dengue, upload_ka_dengue)

epipipeline.logging_config.setup_logging('DEBUG')
dataio.logging_config.setup_logging('INFO')

# unzip folder
unzipped_dir = unzip_folder()

# rename files
file_dir = rename_ka_dengue(directory = unzipped_dir)

# upload files
result = upload_ka_dengue(directory = file_dir)

# check if any files are missing/not uploaded
if result != []:
    logging.warning(f"Files not uploaded {result}")
