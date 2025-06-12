SELECT *
FROM stedi_project.customer_trusted
WHERE sharewithresearchasofdate IS NOT NULL
LIMIT 10;