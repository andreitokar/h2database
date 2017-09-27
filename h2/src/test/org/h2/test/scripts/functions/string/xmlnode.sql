CALL XMLNODE('a', XMLATTR('href', 'http://h2database.com'));
> STRINGDECODE('<a href=\"http://h2database.com\"/>\n')
> -----------------------------------------------------
> <a href="http://h2database.com"/>
> rows: 1

CALL XMLNODE('br');
> STRINGDECODE('<br/>\n')
> -----------------------
> <br/>
> rows: 1

CALL XMLNODE('p', null, 'Hello World');
> STRINGDECODE('<p>Hello World</p>\n')
> ------------------------------------
> <p>Hello World</p>
> rows: 1

SELECT XMLNODE('p', null, 'Hello' || chr(10) || 'World') X;
> X
> ---------------------
> <p> Hello World </p>
> rows: 1

SELECT XMLNODE('p', null, 'Hello' || chr(10) || 'World', false) X;
> X
> -------------------
> <p>Hello World</p>
> rows: 1

