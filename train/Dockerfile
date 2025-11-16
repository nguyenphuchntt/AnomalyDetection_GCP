FROM python:3.12.4

WORKDIR /root

COPY train.py /root/train.py
COPY register_model.py /root/register_model.py
COPY requirements.txt /root/requirements.txt

RUN pip install -r /root/requirements.txt

ENTRYPOINT ["python", "train.py"]