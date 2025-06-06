FROM openjdk:jre AS widoco

RUN wget https://github.com/dgarijo/Widoco/releases/download/v1.4.25/widoco-1.4.25-jar-with-dependencies_JDK-11.jar 

COPY ./data/all.ttl /data/ontology.ttl

RUN java -jar widoco-1.4.25-jar-with-dependencies_JDK-11.jar -ontFile /data/ontology.ttl -outFolder public -uniteSections -includeAnnotationProperties -lang en-de -getOntologyMetadata -noPlaceHolderText -rewriteAll -webVowl

FROM ghcr.io/epoz/shmarql:v0.56

COPY data /data
COPY docs /src/docs
COPY mkdocs.yml a.yml
RUN python -m shmarql docs_build -f a.yml

RUN mkdir /src/site/ontology
#COPY --from=widoco /public /src/site/ontology
#RUN cp /src/site/ontology/index-en.html /src/site/ontology/index.html
