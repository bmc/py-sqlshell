# This Dockerfile is used to build an image in which I can easily test
# sqlshell on a Linux distribution.

FROM debian:latest

RUN apt-get update
RUN apt -y install git
RUN apt -y install python3
RUN apt -y install python3-pip
RUN apt -y install python3-venv
RUN apt -y install man
RUN apt -y install sudo
RUN apt -y install vim
RUN apt -y install zsh
RUN useradd -ms /usr/bin/zsh bmc
USER bmc
WORKDIR /home/bmc
COPY inputrc /home/bmc/.inputrc
COPY editrc /home/bmc/.editrc
COPY zshrc /home/bmc/.zshrc
COPY sqlshell.cfg /home/bmc/.sqlshell.cfg
RUN /usr/bin/python3 -m venv python
RUN sh -c ". ./python/bin/activate; pip install build"
RUN git clone https://github.com/bmc/py-sqlshell
RUN sh -c ". ./python/bin/activate; cd py-sqlshell; ./build.sh build"
RUN sh -c ". ./python/bin/activate; pip install py-sqlshell/dist/*.whl"
CMD /usr/bin/zsh -i
