FROM python:3

RUN pip install grpcio
RUN pip install grpcio-tools

RUN mkdir -p /app

COPY restaurant_service.py /app/

ENV PYTHONPATH /app:/app/event_store:/app/message_queue

RUN git clone ... /app/restaurant_service.py
RUN git clone ... /app/message_queue

CMD [ "python", "app/restaurant_service.py" ]
