FROM mcr.microsoft.com/mssql-tools

ENV LOGIN=sa
ENV PASSWORD=sa

COPY database/create_database.sql /create_database.sql
COPY database/create_table.sql /create_table.sql
COPY database/create_user.sql /create_user.sql

CMD ["sh", "-c", "/opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U $LOGIN -P $PASSWORD -i /create_database.sql && /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U $LOGIN -P $PASSWORD -i /create_table.sql && /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U $LOGIN -P $PASSWORD -i /create_user.sql"]
