FROM node:16-alpine

RUN apk add git
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools

RUN git clone https://github.com/bbc/cucumber-slicer.git
WORKDIR cucumber-slicer
RUN npm install

COPY docker/slicer/DocStringSupport.patch ./
COPY docker/slicer/slicer.js ./

RUN git apply DocStringSupport.patch


RUN mkdir features generatedFeatures
ENTRYPOINT tar -C features -xv &>/dev/null; \
    node slicer.js; \
    (cd generatedFeatures; tar -c .)