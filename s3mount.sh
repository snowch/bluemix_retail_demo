# change URL to reflect your environment

s3fs temp-bucket ./s3mount -o passwd_file=./config/s3_password -o url=https://s3.eu-geo.objectstorage.softlayer.net -o use_path_request_style -o uid=$(id -u),umask=077,gid=$(id -g)
