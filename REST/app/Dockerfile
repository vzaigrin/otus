FROM photon
RUN tdnf install -y python3-pip libstdc++-devel
ENV FLASK_ENV=development
RUN mkdir -p /app
COPY app.py requirements.txt /app/
WORKDIR /app
RUN pip3 install --no-cache-dir -r requirements.txt
EXPOSE 5000
ENTRYPOINT ["python3"]
CMD ["app.py"]
