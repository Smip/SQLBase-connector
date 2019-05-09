<?php

namespace SQLBase;

class SQLBase
{
    private $dbconn;
    private $emode;


    public function __construct($server, $user, $pass) {
        $this->emode = "error";
        $this->dbconn = odbc_connect($server, $user, $pass);
        if(!$this->dbconn){
            die('Could not connect: ' . odbc_errormsg());
        }
    }
}
