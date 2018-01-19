FROM registry.docker-cn.com/library/python:3.6

RUN apt-get update
RUN apt-get install -y libsnappy-dev

WORKDIR /project

ENV PYTHONPATH=/project:$PYTHONPATH

COPY requirements.txt ./

RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY .  ./

RUN python setup.py install

RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

# ENV LOG_DIR="/log"
VOLUME ["/conf", "/log"]
EXPOSE 23000
WORKDIR /conf

CMD ["jaqsmds", "server"]