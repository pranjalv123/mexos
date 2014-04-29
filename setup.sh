sudo apt-get install leveldb-dbg
wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
tar xvf https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
cat "GOROOT=/home/ubuntu/go" >> /home/ubuntu/.bashrc
cat "PATH=$PATH:$GOROOT/bin" >> /home/ubuntu/.bashrc
cat "GOPATH=/home/ubuntu/mexos" >> /home/ubuntu/.bashc

source .bashrc

