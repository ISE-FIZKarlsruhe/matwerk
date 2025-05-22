@echo off
REM Set paths
set COMPONENTSDIR=data
set VALIDATIONSDIR=data/validation
set SRC=ontology/mwo-full.owl
set ROBOT_IMAGE=obolibrary/robot
set ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% --inputs "data/components/*.owl" --output %COMPONENTSDIR%/all_NotReasoned.owl

::powershell -Command "(Get-Content 'data\all_NotReasoned.owl' -Raw) -replace '<ontology:NFDI_0001008>(.*?)</ontology:NFDI_0001008>', '<ontology:NFDI_0001008 rdf:resource=\"$1\"/>' | Set-Content 'data\all_NotReasoned.owl'"
powershell -Command "(Get-Content 'data\all_NotReasoned.owl' -Raw) -replace '<ontology:NFDI_0001008>', '<ontology:NFDI_0001008 rdf:datatype=\"http://www.w3.org/2001/XMLSchema#anyURI\">' | Set-Content 'data\all_NotReasoned.owl'"

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --input %COMPONENTSDIR%/all_NotReasoned.owl -M inconsistency --explanation %VALIDATIONSDIR%/inconsistency.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %SRC% -M inconsistency --explanation %VALIDATIONSDIR%/inconsistency_mwo.md


docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/all_NotReasoned.owl -M inconsistency --explanation %VALIDATIONSDIR%/inconsistency_hermit.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
      %ROBOT_IMAGE% robot reason --reasoner hermit --input %COMPONENTSDIR%/all_NotReasoned.owl --axiom-generators "SubClass ClassAssertion" --output %COMPONENTSDIR%/all.ttl
::    %ROBOT_IMAGE% robot reason --reasoner hermit --input %COMPONENTSDIR%/all_NotReasoned.owl --axiom-generators "SubClass EquivalentClass DisjointClasses ClassAssertion PropertyAssertion SubObjectProperty EquivalentObjectProperty InverseObjectProperties ObjectPropertyDomain ObjectPropertyRange ObjectPropertyCharacteristic SubDataProperty EquivalentDataProperties DataPropertyCharacteristic" --include-indirect true reduce --output %COMPONENTSDIR%/all.ttl

python3 -m pyshacl  -s shapes/shape4.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape4.md

echo  no roles without bearers
python3 -m pyshacl  -s shapes/shape3.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape3.md

echo  no punning
python3 -m pyshacl  -s shapes/shape2.ttl %COMPONENTSDIR%/all.ttl  > %VALIDATIONSDIR%/shape2.md

echo  no orphaned textual entities
python3 -m pyshacl  -s shapes/shape1.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape1.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot verify --input %COMPONENTSDIR%/all.ttl --queries shapes/verify1.sparql --output-dir data/validation/ -vvv > %VALIDATIONSDIR%/verify1.md