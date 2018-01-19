FROM registry.docker-cn.com/library/python:3.6

RUN apt-get update
RUN apt-get install -y libsnappy-dev
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple git+https://github.com/cheatm/jaqsmds.git
RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

# ENV LOG_DIR="/log"
VOLUME ["/config", "/log"]
EXPOSE 23000
WORKDIR /config

CMD ["jaqsmds", "server"]
