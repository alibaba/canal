
# osbase
cd amd64 && docker build --no-cache -t canal/osbase ./ -f Dockerfile

# osadmin
cd amd64 && docker build --no-cache -t canal/osadmin ./ -f ./Dockerfile_admin
