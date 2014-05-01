#ssh-keygen -f "/home/pranjal/.ssh/known_hosts" -R 54.86.104.17
ssh -A -i mexos-keypair.pem ubuntu@54.86.104.17
