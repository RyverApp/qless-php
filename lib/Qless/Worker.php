<?php

namespace Qless;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Worker
{
    public const PROCESS_TYPE_MASTER   = 0;
    public const PROCESS_TYPE_JOB      = 1;
    public const PROCESS_TYPE_WATCHDOG = 2;

    /**
     * @var Queue[]
     */
    private $queues = [];

    /**
     * @var int
     */
    private $interval;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var bool
     */
    private $shutdown = false;

    /**
     * @var string
     */
    private $workerName;

    /**
     * @var int|null
     */
    private $childPID;

    /**
     * @var int|null
     */
    private $watchdogPID;

    /**
     * @var int
     */
    private $childProcesses = 0;

    /**
     * @var string
     */
    private $jobPerformClass;

    /**
     * @var bool
     */
    private $paused = false;

    /**
     * @var string
     */
    private $who = 'master';

    /**
     * @var array
     */
    private $logCtx;

    /**
     * @var \Socket[]
     */
    private $sockets = [];

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Job|null
     */
    private $job;

    public function __construct($name, $queues, Client $client, $interval = 60)
    {
        $this->workerName = $name;
        $this->client     = $client;
        $this->interval   = $interval;
        foreach ($queues as $queue) {
            $this->queues[] = $this->client->getQueue($queue);
        }
        $this->logger = new NullLogger();
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    public function registerJobPerformHandler($klass): void
    {
        if (!class_exists($klass)) {
            throw new \Exception("Could not find job perform class $klass");
        }

        if (!method_exists($klass, 'perform')) {
            throw new \Exception("Job class $klass does not contain perform method");
        }

        $this->jobPerformClass = $klass;
    }

    private static $ERROR_CODES = [
        E_ERROR         => 'E_ERROR',
        E_CORE_ERROR    => 'E_CORE_ERROR',
        E_COMPILE_ERROR => 'E_COMPILE_ERROR',
        E_PARSE         => 'E_PARSE',
        E_USER_ERROR    => 'E_USER_ERROR',
    ];

    public function run(): void
    {
        declare(ticks=1);

        $this->masterRegisterSigHandlers();
        $this->who    = "master:$this->workerName";
        $queues       = implode(', ', $this->queues);
        $this->logCtx = [];
        $this->logger->info("[$this->who] worker started, monitoring queues $queues", $this->logCtx);

        $did_work = false;

        while (true) {
            if ($this->shutdown) {
                $this->logger->info("[$this->who] shutting down", $this->logCtx);
                break;
            }

            while ($this->paused) {
                usleep(250000);
            }

            if ($did_work) {
                $queues = implode(',', $this->queues);
                $now    = strftime('%F %T');
                $this->updateProcLine("Waiting for $queues with interval $this->interval since $now");
                $this->logger->debug("[$this->who] waiting for work", $this->logCtx);
                $did_work = false;
            }

            $job = $this->reserve();
            if (!$job) {
                if ($this->interval === 0) {
                    break;
                }
                sleep($this->interval);
                continue;
            }

            $this->job                      = $job;
            $this->logCtx['job_identifier'] = $job->getId();
            $this->logCtx['job_queue']      = $job->getQueueName();

            // fork processes
            $this->childStart();
            $this->watchdogStart();

            // Parent process, sit and wait
            $now = strftime('%F %T');
            $this->updateProcLine("Forked $this->childPID at $now");
            $this->logger->info("[$this->who] forked child", $this->logCtx);

            while ($this->childProcesses > 0) {
                $status = null;
                $pid    = pcntl_wait($status, WUNTRACED);
                if ($pid > 0) {
                    if ($pid === $this->childPID) {
                        $exited = $this->childProcessStatus($status);
                    } elseif ($pid === $this->watchdogPID) {
                        $exited = $this->watchdogProcessStatus($status);
                    } else {
                        // unexpected?
                        $this->logger->info("[$this->who] received status for unknown PID $pid; exiting", $this->logCtx);
                        exit(1);
                    }

                    if ($exited) {
                        --$this->childProcesses;
                        switch ($pid) {
                            case $this->childPID:
                                $this->childPID = null;
                                if ($this->watchdogPID) {
                                    // shutdown watchdog immediately if child has exited
                                    posix_kill($this->watchdogPID, SIGKILL);
                                }
                                break;

                            case $this->watchdogPID:
                                $this->watchdogPID = null;
                                break;
                        }
                    }
                }
            }

            foreach ($this->sockets as $socket) {
                socket_close($socket);
            }
            $this->sockets = [];
            $this->job     = null;
            unset($this->logCtx['job_identifier'], $this->logCtx['job_queue']);
            $did_work = true;

            /**
             * We need to reconnect due to bug in Redis library that always sends QUIT on destruction of \Redis
             * rather than just leaving socket around. This call will sometimes generate a broken pipe notice
             */
            $old = error_reporting();
            error_reporting($old & ~E_NOTICE);
            try {
                $this->client->reconnect();
            } finally {
                error_reporting($old);
            }
        }
    }

    /**
     * @return bool|Job
     */
    private function reserve()
    {
        foreach ($this->queues as $queue) {
            try {
                if ($job = $queue->pop($this->workerName)) {
                    return $job[0];
                }
            } catch (QlessException $e) {
                $error = $e->getMessage();
                $this->logger->error("[$this->who] unable to reserve job on queue $queue: $error", $this->logCtx);
            }
        }
        return false;
    }

    /**
     * Register signal handlers that a worker should respond to.
     *
     * TERM: Shutdown immediately and stop processing jobs.
     * INT: Shutdown immediately and stop processing jobs.
     * QUIT: Shutdown after the current job finishes processing.
     * USR1: Kill the forked child immediately and continue processing jobs.
     */
    private function masterRegisterSigHandlers(): void
    {
        pcntl_signal(SIGTERM, function (): void {
            $this->shutdownNow();
        }, false);
        pcntl_signal(SIGINT, function (): void {
            $this->shutdownNow();
        }, false);
        pcntl_signal(SIGQUIT, function (): void {
            $this->shutdown();
        }, false);
        pcntl_signal(SIGUSR1, function (): void {
            $this->killChildren();
        }, false);
        pcntl_signal(SIGUSR2, function (): void {
            $this->pauseProcessing();
        }, false);
        pcntl_signal(SIGCONT, function (): void {
            $this->unPauseProcessing();
        }, false);
    }

    /**
     * Clear all previously registered signal handlers
     */
    private function clearSigHandlers(): void
    {
        pcntl_signal(SIGTERM, SIG_DFL);
        pcntl_signal(SIGINT, SIG_DFL);
        pcntl_signal(SIGQUIT, SIG_DFL);
        pcntl_signal(SIGUSR1, SIG_DFL);
        pcntl_signal(SIGUSR2, SIG_DFL);
        pcntl_signal(SIGCONT, SIG_DFL);
    }

    /**
     * Forks and creates a socket pair for communication between parent and child process
     *
     * @param \Socket|null $socket
     *
     * @return int PID if master or 0 if child
     */
    private function fork(&$socket)
    {
        $pair = [];
        if (socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair) === false) {
            $error = socket_strerror(socket_last_error($pair[0]));
            $this->logger->error("[$this->who] unable to create socket pair: $error", $this->logCtx);

            exit(0);
        }

        $PID = Qless::fork();
        if ($PID !== 0) {
            // MASTER
            $this->childProcesses++;

            $socket = $pair[0];
            socket_close($pair[1]);
            socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => 0, 'usec' => 10000]); // wait up to 10ms to receive data

            return $PID;
        }

        $socket = $pair[1];
        socket_close($pair[0]);

        $reserved = str_repeat('x', 20240);

        register_shutdown_function(function () use (&$reserved, $socket): void {
            // shutting down
            if (null === $error = error_get_last()) {
                return;
            }
            unset($reserved);

            $type = $error['type'];
            if (!isset(self::$ERROR_CODES[$type])) {
                return;
            }

            $this->logger->debug("[$this->who] sending error to master", $this->logCtx);
            $data = serialize($error);

            while (($len = socket_write($socket, $data)) > 0) {
                $data = substr($data, $len);
            }
        });

        return $PID;
    }

    /**
     * @param \Socket $socket
     *
     * @return null|string
     */
    private function readErrorFromSocket($socket)
    {
        $error_info = '';
        while (!empty($res = socket_read($socket, 8192))) {
            $error_info .= $res;
        }
        $error_info = unserialize($error_info);
        if (is_array($error_info)) {
            return sprintf('[%s] %s:%d %s', self::$ERROR_CODES[$error_info['type']], $error_info['file'], $error_info['line'], $error_info['message']);
        }

        return null;
    }

    /**
     * @param int $PID
     * @param int $child_type
     * @param int $exitStatus
     *
     * @return bool|string false if exit status indicates success; otherwise, a string containing the error messages
     */
    private function handleProcessExitStatus($PID, $child_type, $exitStatus)
    {
        $child_type = $child_type === self::PROCESS_TYPE_JOB ? 'child' : 'watchdog';

        if ($exitStatus === 0) {
            $this->logger->debug("[$this->who] $child_type process exited successfully", $this->logCtx);

            return false;
        }

        $jobFailedMessage = $this->readErrorFromSocket($this->sockets[$PID]) ?: "$child_type process failed with status: $exitStatus";
        $this->logger->error("[$this->who] fatal error in $child_type process: $jobFailedMessage", $this->logCtx);

        return $jobFailedMessage;
    }

    private function childStart(): void
    {
        $socket         = null;
        $this->childPID = $this->fork($socket);
        if ($this->childPID !== 0) {
            $this->sockets[$this->childPID] = $socket;
            return;
        }

        $this->clearSigHandlers();

        $jid          = $this->job->getId();
        $now          = strftime('%F %T');
        $this->who    = "child:$this->workerName";
        $this->logCtx = ['job_identifier' => $jid, 'job_queue' => $this->job->getQueueName()];
        $this->updateProcLine("Processing $jid since $now");
        $this->logger->info("[$this->who] processing child", $this->logCtx);
        $this->childPerform($this->job);

        socket_close($socket);

        exit(0);
    }

    /**
     * Process a single job.
     *
     * @param Job $job The job to be processed.
     */
    private function childPerform(Job $job): void
    {
        try {
            if ($this->jobPerformClass) {
                $performClass = new $this->jobPerformClass();
                $performClass->perform($job);
            } else {
                $job->perform();
            }
            $this->logger->notice("[$this->who] job finished", $this->logCtx);
        } catch (\Exception $e) {
            $error = $e->getMessage();
            $this->logger->critical("[$this->who] job failed: $error", $this->logCtx);
            $job->fail('system:fatal', $error);
        }
    }

    /**
     * @param int $status
     *
     * @return bool
     */
    private function childProcessStatus($status)
    {
        switch (true) {
            case pcntl_wifexited($status):
                $code = pcntl_wexitstatus($status);
                $res  = $this->handleProcessExitStatus($this->childPID, self::PROCESS_TYPE_JOB, $code);
                if ($res !== false) {
                    $this->job->fail('system:fatal', $res);
                }
                return true;

            case pcntl_wifsignaled($status):
                $sig = pcntl_wtermsig($status);
                if ($sig !== SIGKILL) {
                    $sig = pcntl_sig_name($sig);
                    $this->logger->warning("[$this->who] child $this->childPID terminated with unhandled signal $sig", $this->logCtx);
                }
                return true;

            case pcntl_wifstopped($status):
                $sig = pcntl_sig_name(pcntl_wstopsig($status));
                $this->logger->info("[$this->who] child $this->childPID stopped with signal $sig", $this->logCtx);
                return false;

            default:
                $this->logger->error("[$this->who] unexpected status for child $this->childPID; exiting", $this->logCtx);
                exit(1);
        }
    }

    private function childKill(): void
    {
        if ($this->childPID) {
            $this->logger->info("[$this->who] killing child $this->childPID", $this->logCtx);
            if (pcntl_waitpid($this->childPID, $status, WNOHANG) !== -1) {
                posix_kill($this->childPID, SIGKILL);
            }
            $this->childPID = null;
        }
    }

    private function watchdogStart(): void
    {
        $socket            = null;
        $this->watchdogPID = $this->fork($socket);
        if ($this->watchdogPID !== 0) {
            $this->sockets[$this->watchdogPID] = $socket;
            return;
        }

        $this->clearSigHandlers();

        $jid          = $this->job->getId();
        $now          = strftime('%F %T');
        $this->who    = "watchdog:$this->workerName";
        $this->logCtx = ['job_identifier' => $jid, 'job_queue' => $this->job->getQueueName()];
        $this->updateProcLine("Watching events for $jid since $now");
        $this->logger->info("[$this->who] watching events", $this->logCtx);

        ini_set('default_socket_timeout', -1);
        $l = $this->client->createListener(['ql:log']);
        $l->messages(function ($channel, $event) use ($l, $jid): void {
            if ($event->jid !== $jid || !in_array($event->event, ['lock_lost', 'canceled', 'completed', 'failed'])) {
                return;
            }

            switch ($event->event) {
                case 'lock_lost':
                    if ($event->worker === $this->workerName) {
                        $this->logger->info("[$this->who] job handed to another worker; killing child $this->childPID", $this->logCtx);
                        posix_kill($this->childPID, SIGKILL);
                        $l->stop();
                    }
                    break;

                case 'canceled':
                    if ($event->worker === $this->workerName) {
                        $this->logger->info("[$this->who] job canceled; killing child $this->childPID", $this->logCtx);
                        posix_kill($this->childPID, SIGKILL);
                        $l->stop();
                    }
                    break;

                case 'completed':
                case 'failed':
                    $l->stop();
                    break;
            }
        });

        socket_close($socket);
        $this->logger->info("[$this->who] watchdog done", $this->logCtx);

        exit(0);
    }

    /**
     * @param int $status
     *
     * @return bool
     */
    private function watchdogProcessStatus($status)
    {
        switch (true) {
            case pcntl_wifexited($status):
                $code = pcntl_wexitstatus($status);
                $this->handleProcessExitStatus($this->watchdogPID, self::PROCESS_TYPE_WATCHDOG, $code);
                return true;

            case pcntl_wifsignaled($status):
                $sig = pcntl_wtermsig($status);
                if ($sig !== SIGKILL) {
                    $sig = pcntl_sig_name($sig);
                    $this->logger->warning("[$this->who] watchdog $this->watchdogPID terminated with unhandled signal $sig", $this->logCtx);
                }
                return true;

            case pcntl_wifstopped($status):
                $sig = pcntl_sig_name(pcntl_wstopsig($status));
                $this->logger->info("[$this->who] watchdog $this->watchdogPID stopped with signal $sig", $this->logCtx);
                return false;

            default:
                $this->logger->error("[$this->who] unexpected status for watchdog $this->watchdogPID; exiting", $this->logCtx);
                exit(1);
        }
    }

    private function watchdogKill(): void
    {
        if ($this->watchdogPID) {
            $this->logger->info("[$this->who] killing watchdog $this->watchdogPID", $this->logCtx);
            if (pcntl_waitpid($this->watchdogPID, $status, WNOHANG) !== -1) {
                posix_kill($this->watchdogPID, SIGKILL);
            }
            $this->watchdogPID = null;
        }
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     */
    public function pauseProcessing(): void
    {
        $this->logger->notice("[$this->who] USR2 received; pausing job processing", $this->logCtx);
        $this->paused = true;
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     */
    public function unPauseProcessing(): void
    {
        $this->logger->notice("[$this->who] CONT received; resuming job processing", $this->logCtx);
        $this->paused = false;
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     */
    public function shutdown(): void
    {
        if ($this->childPID) {
            $this->logger->notice("[$this->who] QUIT received; shutting down after child completes work", $this->logCtx);
        } else {
            $this->logger->notice("[$this->who] QUIT received; shutting down", $this->logCtx);
        }
        $this->shutdown = true;
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     */
    public function shutdownNow(): void
    {
        $this->logger->notice("[$this->who] TERM or INT received; shutting down immediately", $this->logCtx);
        $this->shutdown = true;
        $this->killChildren();
    }

    /**
     * Kill a forked child job immediately. The job it is processing will not
     * be completed.
     */
    public function killChildren(): void
    {
        if (!$this->childPID && !$this->watchdogPID) {
            return;
        }

        $this->childKill();
        $this->watchdogKill();
    }

    private function updateProcLine($status): void
    {
        $version = Qless::VERSION;
        cli_set_process_title("qless-$version: $status");
    }
}

function pcntl_sig_name($sig_no)
{
    static $pcntl_consts;
    if (!isset($pcntl_consts)) {
        $a            = get_defined_constants(true)['pcntl'];
        $f            = array_filter(array_keys($a), fn ($k) => str_starts_with($k, 'SIG') && !str_contains($k, 'SIG_'));
        $pcntl_consts = array_flip(array_intersect_key($a, array_flip($f)));
        unset($a, $f);
    }

    return $pcntl_consts[$sig_no] ?? 'UNKNOWN';
}
