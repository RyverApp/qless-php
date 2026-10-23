<?php

require_once __DIR__ . '/../lib/Qless/Lua.php';

class LuaTester extends \Qless\Lua
{
    public $time = 0;

    public function run($command, $args)
    {
        if (empty($this->sha)) {
            $this->reload();
        }
        $luaArgs  = [$command, $this->time];
        $argArray = array_merge($luaArgs, $args);
        $result   = $this->redisCli->evalSha($this->sha, $argArray);
        if ($error = $this->redisCli->getLastError()) {
            $this->handleError($error);
            return null;
        }

        return $result;
    }
}
