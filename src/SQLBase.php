<?php

namespace SQLBase;

class SQLBase
{

    private $dbconn;
    private $dsn;
    private $username;
    private $password;
    private $schema;
    private $stats = [];

    /**
     * 
     * @param type $configuration
     */
    public function __construct($configuration) {
//        'dsn' => '',
//        'username' => '',
//        'password' => '',
//        'schema' => ''

        $this->dsn = $configuration['dsn'];
        $this->username = $configuration['username'];
        $this->password = $configuration['password'];
        $this->schema = $configuration['schema'];
    }

    private function connect() {
        if (!$this->dbconn) {
            $this->dbconn = odbc_connect($this->dsn, $this->username, $this->password);
            if (!$this->dbconn) {
                throw new \Exception('Could not connect: ' . odbc_errormsg());
            }
        }
    }

    function __destruct() {
        if ($this->dbconn) {
            odbc_close($this->dbconn);
        }
    }

    public function fetch($result) {
        $res = odbc_fetch_array($result);
        if ($res) {
            foreach ($res as $key => &$value) {
                $coded_res[preg_replace("/.*\./", "", strtolower($key))] = iconv("Windows-1250", "UTF-8", $value);
            }
        }
        if (!empty($coded_res)) {
            return $coded_res;
        } else {
            return $res;
        }
    }

    /**
     * 
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed
     */
    public function getAll() {
        $stat = [];
        $start = microtime(TRUE);
        $query = $this->prepareQuery(func_get_args());
        $stat['query'] = $query;

        if (preg_match("/LIMIT (\d+)/", $query, $res) and $res[1]) {
            $query = preg_replace("/LIMIT \d+/", "", $query);
            $limit = (int) $res[1];
        } else {
            $limit = null;
        }

        if (preg_match("/OFFSET (\d+)/", $query, $res) and isset($res[1])) {
            $query = preg_replace("/OFFSET \d+/", "", $query);
            $offset = $res[1];
        } else {
            $offset = 0;
        }

        $collection = collect([]);
        if ($res = $this->rawQuery($query)) {
            while ($row = $this->fetch($res) and $collection->count() < $limit) {
                if ($offset) {
                    $offset -= 1;
                    continue;
                }
                $collection->push($row);
            }
            $this->free($res);
        }
        $stat['time'] = microtime(TRUE) - $start;
        $this->stats[] = $stat;
        return $collection;
    }

    /**
     * 
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed
     */
    public function getCol() {
        $ret = array();
        $query = $this->prepareQuery(func_get_args());
        if (preg_match("/LIMIT (\d+)/", $query, $limit) and $limit[1]) {
            $query = preg_replace("/LIMIT (\d+)/", "", $query);
        } else {
            $limit[1] = null;
        }
        if (preg_match("/OFFSET (\d+)/", $query, $OFFSET) and $OFFSET[1]) {
            $query = preg_replace("/OFFSET (\d+)/", "", $query);
        } else {
            $OFFSET[1] = 0;
        }
        if ($res = $this->rawQuery($query)) {
            while ($row = $this->fetch($res)) {
                $ret[] = reset($row);
            }
            $this->free($res);
        }
        return array_slice($ret, $OFFSET[1], $limit[1], TRUE);
    }

    /**
     * 
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed
     */
    public function getOne() {

        $query = $this->prepareQuery(func_get_args());

        if ($res = $this->rawQuery($query)) {
            $row = $this->fetch($res);
            if (is_array($row)) {
                return reset($row);
            }
            $this->free($res);
        }
        return FALSE;
    }

    /**
     * 
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed
     */
    public function getRow() {
        $start = microtime(TRUE);
        $stat = [];
        $query = $this->prepareQuery(func_get_args());
        $stat['query'] = $query;

        if (preg_match("/LIMIT (\d+)/", $query, $res) and $res[1]) {
            $query = preg_replace("/LIMIT \d+/", "", $query);
        }


        if (preg_match("/OFFSET (\d+)/", $query, $res) and isset($res[1])) {
            $query = preg_replace("/OFFSET \d+/", "", $query);
            $offset = $res[1];
        } else {
            $offset = 0;
        }
        if ($res = $this->rawQuery($query)) {
            while ($row = $this->fetch($res)) {
                if ($offset) {
                    $offset -= 1;
                    continue;
                }
                $collection = collect($row);
                break;
            }
            $this->free($res);
        }
        $stat['time'] = microtime(TRUE) - $start;
        $this->stats[] = $stat;
        return $collection;



        $query = $this->prepareQuery(func_get_args());
        if ($res = $this->rawQuery($query)) {
            $ret = $this->fetch($res);
            $this->free($res);
            return $ret;
        }
        return FALSE;
    }

    /**
     * 
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed
     */
    public function query() {
        return $this->rawQuery($this->prepareQuery(func_get_args()));
    }

    /**
     * 
     * 
     * @param string $key key
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed two-dimensional array indexed by the values of the field specified by the first parameter
     */
    public function getInd() {
        $args = func_get_args();
        $index = array_shift($args);
        $query = $this->prepareQuery($args);

        if (preg_match("/LIMIT (\d+)/", $query, $limit) and $limit[1]) {
            $query = preg_replace("/LIMIT (\d+)/", "", $query);
        } else {
            $limit[1] = null;
        }

        if (preg_match("/OFFSET (\d+)/", $query, $OFFSET) and $OFFSET[1]) {
            $query = preg_replace("/OFFSET (\d+)/", "", $query);
        } else {
            $OFFSET[1] = 0;
        }
        $ret = array();

        if ($res = $this->rawQuery($query)) {
            while ($row = $this->fetch($res)) {
                $ret[$row[$index]] = $row;
            }
            $this->free($res);
        }

        return array_slice($ret, $OFFSET[1], $limit[1], TRUE);
    }

    /**
     * 
     * @param string $key key
     * @param string $sql sql-query
     * @param mixed ... variables to binding 
     * @return mixed array of scalars, indexed by the field from the first parameter. Indispensable for compiling dictionaries of the form key => value
     */
    public function getIndCol() {
        $args = func_get_args();
        $index = array_shift($args);
        $query = $this->prepareQuery($args);
        preg_match("/LIMIT (\d+)/", $query, $limit);
        if ($limit[1]) {
            $query = preg_replace("/LIMIT (\d+)/", "", $query);
        } else {
            $limit[1] = null;
        }
        preg_match("/OFFSET (\d+)/", $query, $OFFSET);
        if ($OFFSET[1]) {
            $query = preg_replace("/OFFSET (\d+)/", "", $query);
        } else {
            $OFFSET[1] = 0;
        }
        $ret = array();
        if ($res = $this->rawQuery($query)) {
            while ($row = $this->fetch($res)) {
                $key = $row[$index];
                unset($row[$index]);
                $ret[$key] = reset($row);
            }
            $this->free($res);
        }

        return array_slice($ret, $OFFSET[1], $limit[1], TRUE);
    }

    private function rawQuery($query) {
        $this->connect();
        $res = odbc_exec($this->dbconn, $query);
        if (!$res) {
            $error = odbc_error($this->dbconn);
            $this->cutStats();
            $this->error("$error. Full query: [$query]");
        }
        $this->cutStats();
        return $res;
    }

    public function getQueryLog() {
        return $this->stats;
    }

    public function parse() {
        return $this->prepareQuery(func_get_args());
    }

    private function cutStats() {
        if (count($this->stats) > 100) {
            reset($this->stats);
            $first = key($this->stats);
            unset($this->stats[$first]);
        }
    }

    private function prepareQuery($args) {
        $query = '';
        $raw = array_shift($args);
        $array = preg_split('~(\?[nsiuap])~u', $raw, null, PREG_SPLIT_DELIM_CAPTURE);
        $anum = count($args);
        $pnum = floor(count($array) / 2);
        if ($pnum != $anum) {
            $this->error("Number of args ($anum) doesn't match number of placeholders ($pnum) in [$raw]");
        }
        foreach ($array as $i => $part) {
            if (($i % 2) == 0) {
                $query .= $part;
                continue;
            }
            $value = array_shift($args);
            switch ($part) {
                case '?n':
                    $part = $this->escapeIdent($value);
                    break;
                case '?s':
                    $part = $this->escapeString($value);
                    break;
                case '?i':
                    $part = $this->escapeInt($value);
                    break;
                case '?a':
                    $part = $this->createIN($value);
                    break;
                case '?u':
                    $part = $this->createSET($value);
                    break;
                case '?p':
                    $part = $value;
                    break;
            }
            $query .= $part;
        }
        $query = str_replace("NOW()", date("Y-m-d H:i:s"), $query);
        preg_match_all("/FROM\s+(.*?)(?:\s+where|\s\)|$|\s+order|\s+limit)/is", $query, $matches, PREG_OFFSET_CAPTURE);
        $correction = 0;
        foreach ($matches[1] as $match) {
            $tables = explode(',', $match[0]);
            $newTables = [];
            foreach ($tables as $tableName) {
                if (strpos($tableName, '.') === false) {
                    $tableName = $this->schema . "." . trim($tableName);
                }
                $newTables[] = $tableName;
            }
            $tableList = implode(", ", $newTables);
            $query = substr_replace($query, $tableList, $match[1] + $correction, strlen($match[0]));
            $correction += strlen($tableList) - strlen($match[0]);
        }

        return $query;
    }

    private function escapeInt($value) {
        if ($value === NULL) {
            return 'NULL';
        }
        if (!is_numeric($value)) {
            $this->error("Integer (?i) placeholder expects numeric value, " . gettype($value) . " given");
            return FALSE;
        }
        if (is_float($value)) {
            $value = number_format($value, 0, '.', ''); // may lose precision on big numbers
        }
        return $value;
    }

    private function escapeString($value) {
        if ($value === NULL) {
            return 'NULL';
        }
        return "'" . $value . "'";
    }

    private function escapeIdent($value) {
        if ($value) {
            return "`" . str_replace("`", "``", $value) . "`";
        } else {
            $this->error("Empty value for identifier (?n) placeholder");
        }
    }

    private function createIN($data) {
        if (!is_array($data)) {
            $this->error("Value for IN (?a) placeholder should be array");
            return;
        }
        if (!$data) {
            return 'NULL';
        }
        $query = $comma = '';
        foreach ($data as $value) {
            $query .= $comma . $this->escapeString($value);
            $comma = ",";
        }
        return $query;
    }

    private function createSET($data) {
        if (!is_array($data)) {
            $this->error("SET (?u) placeholder expects array, " . gettype($data) . " given");
            return;
        }
        if (!$data) {
            $this->error("Empty array for SET (?u) placeholder");
            return;
        }
        $query = $comma = '';
        foreach ($data as $key => $value) {
            $query .= $comma . $this->escapeIdent($key) . '=' . $this->escapeString($value);
            $comma = ",";
        }
        return $query;
    }

    private function error($err) {
        $err .= ". Error initiated in " . $this->caller() . ", thrown";
        throw new \Exception($err);
    }

    private function caller() {
        $trace = debug_backtrace();
        $caller = '';
        foreach ($trace as $t) {
            if (isset($t['class']) && $t['class'] == __CLASS__) {
                $caller = $t['file'] . " on line " . $t['line'];
            } else {
                break;
            }
        }
        return $caller;
    }

    public function free($result) {
        odbc_free_result($result);
    }

}
