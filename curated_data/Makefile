.PHONY: stage_state_code

stage_state_code:
	sqlplus "${USERNAME}"/"${PASSWORD}"@"${SID}" @state_code_ddl.sql
	cp state_code.csv state_code.dat
	sqlldr "${USERNAME}"/"${PASSWORD}"@"${SID}" control=state_code.ctl
