FROM python:3
ENV FLASK_ENV=development
RUN mkdir -p /app
COPY . /app
COPY requirements.txt /app
WORKDIR /app
RUN pip3 install --no-cache-dir -r requirements.txt
EXPOSE 5000
ENTRYPOINT ["python3"]
CMD ["app.py"]
