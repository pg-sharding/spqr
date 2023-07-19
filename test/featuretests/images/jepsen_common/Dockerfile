FROM mysync-test-base:latest
RUN (yes | ssh-keygen -t rsa -N '' -f /root/test_ssh_key -C jepsen || true) && \
         eval `ssh-agent -s` && cp /root/test_ssh_key.pub /root/.ssh/authorized_keys && \
                                cp /root/test_ssh_key.pub /root/.ssh/id_rsa.pub && \
                                cp /root/test_ssh_key /root/.ssh/id_rsa && ssh-add -k /root/.ssh/id_rsa

COPY ./ssh_config /etc/ssh/ssh_config
