FROM python:3.9.6-slim-buster

WORKDIR /usr/src/app
RUN pip install --upgrade pip
COPY ./ragamuffin_files/ ./ragamuffin_files/
RUN pip install --no-cache-dir -r ./ragamuffin_files/requirements.txt
COPY ../ragamuffin_core/ /usr/src/app/ragamuffin_core/
RUN pip install --no-cache-dir -r ./ragamuffin_core/requirements.txt

ENV PORT 8000
ENV CURRENT_HOST 0.0.0.0
ENV WORKERS 1
CMD ["sh", "-c", "cd ragamuffin_files && uvicorn app:app --loop asyncio --reload --workers ${WORKERS} --host $CURRENT_HOST --port $PORT --proxy-headers"]