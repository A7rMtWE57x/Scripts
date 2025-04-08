select last_call_et, status, event, sid||','||serial# sid_serial, username, osuser, machine, program, sql_id, module, action, client_info, wait_class, inst_id, status --,t1.*
    from gv$session t1
   where event not in ('rdbms ipc message','jobq slave wait','smon timer','pmon timer','wait for unread message on broadcast channel','Streams AQ: waiting for messages in the queue','Streams AQ: waiting for time management or cleanup tasks',
             'Streams AQ: qmn slave idle wait','Streams AQ: qmn coordinator idle wait','gcs remote message','ges remote message','class slave wait','DIAG idle wait','ASM background timer','PX Deq: Execute Reply',
             'PX Deq: Execution Msg','PX Deq: reap credit','EMON slave idle wait','VKTM Logical Idle Wait','Streams AQ: emn coordinator idle wait','fbar timer','Space Manager: slave idle wait','JOX Jit Process Sleep'
             ,'EMON idle wait','PING','GCR sleep','VKRM Idle') 
             and not (event = 'SQL*Net message from client' and status = 'INACTIVE')
             and type = 'USER'
 order by last_call_et desc;
