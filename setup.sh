sudo apt-get install libleveldb-dev git gcc
git clone https://github.com/pranjalv123/mexos.git
wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
tar xvf go1.2.1.linux-amd64.tar.gz
echo "export GOROOT=/home/ubuntu/go" >> /home/ubuntu/.bashrc
echo "export PATH=\$PATH:\$GOROOT/bin" >> /home/ubuntu/.bashrc
echo "export GOPATH=/home/ubuntu/mexos" >> /home/ubuntu/.bashrc

source .bashrc

