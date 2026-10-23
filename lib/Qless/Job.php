<?php

namespace Qless;

require_once __DIR__ . '/QlessException.php';

class Job
{
    /**
     * @var string
     */
    private $jid;

    /**
     * @var mixed
     */
    private $data;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $queue_name;

    /**
     * @var string
     */
    private $klass_name;

    /**
     * @var string
     */
    private $worker_name;

    /**
     * @var mixed
     */
    private $instance;

    /**
     * @var float
     */
    private $expires;

    /**
     * @var int
     */
    private $priority;

    /**
     * @var string[]
     */
    private $tags;

    /**
     * @var array
     */
    private $job_data;

    public function __construct(Client $client, $job_data)
    {
        $this->client      = $client;
        $this->jid         = $job_data['jid'];
        $this->klass_name  = $job_data['klass'];
        $this->queue_name  = $job_data['queue'];
        $this->data        = json_decode($job_data['data'], true);
        $this->worker_name = $job_data['worker'];
        $this->expires     = $job_data['expires'];
        $this->priority    = $job_data['priority'];
        $this->tags        = $job_data['tags'];
        $this->job_data    = $job_data;
    }

    /**
     * @param Client $client
     * @param array  $job_data
     *
     * @return Job
     */
    public static function fromJobData(Client $client, $job_data)
    {
        return new Job($client, $job_data);
    }

    public function getId()
    {
        return $this->jid;
    }

    /**
     * Seconds remaining before this job will timeout
     *
     * @return float
     */
    public function ttl()
    {
        return $this->expires - microtime(true);
    }

    /**
     * Return the job data
     *
     * @return mixed
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Get the name of the queue this job is on.
     *
     * @return string
     */
    public function getQueueName()
    {
        return $this->job_data['queue'];
    }

    /**
     * Returns a list of jobs which are dependent upon this one completing successfully
     *
     * @return string[]
     */
    public function getDependents()
    {
        return $this->job_data['dependents'];
    }

    /**
     * Returns a list of jobs which must complete successfully before this will be run
     *
     * @return string[]
     */
    public function getDependencies()
    {
        return $this->job_data['dependencies'];
    }

    /**
     * Returns a list of requires resources before this job can be processed
     *
     * @return string[]
     */
    public function getResources()
    {
        return $this->job_data['resources'];
    }

    /**
     * Returns the throttle interval for this job
     *
     * @return float
     */
    public function getInterval()
    {
        return (float)$this->job_data['interval'];
    }

    /**
     * Get the priority of this job
     *
     * @return int
     */
    public function getPriority()
    {
        return $this->priority;
    }

    /**
     * Gets the number of retries remaining for this job
     *
     * @return int
     */
    public function getRetriesLeft()
    {
        return $this->job_data['remaining'];
    }

    /**
     * Gets the number of retries originally requested
     *
     * @return int
     */
    public function getOriginalRetries()
    {
        return $this->job_data['retries'];
    }

    /**
     * Returns the name of the worker currently performing the work or empty
     *
     * @return string
     */
    public function getWorkerName()
    {
        return $this->job_data['worker'];
    }

    /**
     * Get the job history
     *
     * @return array
     */
    public function getHistory()
    {
        return $this->job_data['history'];
    }

    /**
     * Return the current state of the job
     *
     * @return string
     */
    public function getState()
    {
        return $this->job_data['state'];
    }

    /**
     * Get the list of tags associated with this job
     *
     * @return string[]
     */
    public function getTags()
    {
        return $this->tags;
    }

    /**
     * Add the specified tags to this job
     *
     * @param string[] $tags list of tags to remove from this job
     *
     * @return string[] the new list of tags
     */
    public function tag(...$tags)
    {
        return $this->tags = json_decode($this->client->tag('add', $this->jid, ...$tags), true);
    }

    /**
     * Remove the specified tags to this job
     *
     * @param string[] $tags list of tags to add to this job
     *
     * @return string[] the new list of tags
     */
    public function untag(...$tags)
    {
        return $this->tags = json_decode($this->client->tag('remove', $this->jid, ...$tags), true);
    }

    /**
     * Returns the failure information for this job
     *
     * @return array
     */
    public function getFailureInfo()
    {
        return $this->job_data['failure'];
    }

    /**
     * Change the status of this job to complete
     *
     * @param mixed $data
     *
     * @return string
     */
    public function complete($data = null)
    {
        if ($data !== null) {
            $this->data = $data;
            $jsonData   = json_encode($data, JSON_UNESCAPED_SLASHES);
        } else {
            $jsonData = null;
        }

        return $this->client->complete(
            $this->jid,
            $this->worker_name,
            $this->queue_name,
            $jsonData
        );
    }

    /**
     * Options:
     *
     * optional values to replace when re-queuing job
     *
     * * int delay          delay (in seconds)
     * * mixed data         replacement data
     * * int priority       replacement priority
     * * int retries        replacement number of retries
     * * string[] tags      replacement tags
     * * string[] depends   replacement list of JIDs this job is dependent on
     * * string[] resources replacement list of resource IDs required before this job can be processed
     *
     * @param array $opts optional values
     *
     * @return string
     */
    public function requeue($opts = [])
    {
        $jsonData = json_encode($opts['data'] ?? $this->data, JSON_UNESCAPED_SLASHES);

        return $this->client->requeue(
            $this->worker_name,
            $this->queue_name,
            $this->jid,
            $this->klass_name,
            $jsonData,
            $opts['delay'] ?? 0,
            'priority',
            $opts['priority'] ?? $this->priority,
            'tags',
            json_encode($opts['tags'] ?? $this->tags, JSON_UNESCAPED_SLASHES),
            'retries',
            $opts['retries'] ?? $this->getOriginalRetries(),
            'depends',
            json_encode($opts['depends'] ?? $this->getDependencies(), JSON_UNESCAPED_SLASHES),
            'resources',
            json_encode($opts['resources'] ?? $this->getResources(), JSON_UNESCAPED_SLASHES),
            'interval',
            (float)($opts['interval'] ?? $this->getInterval())
        );
    }

    /**
     * Return the job to the work queue for processing
     *
     * @param string $group
     * @param string $message
     * @param int    $delay
     *
     * @return int remaining retries available
     */
    public function retry($group, $message, $delay = 0)
    {
        return $this->client->retry(
            $this->jid,
            $this->queue_name,
            $this->worker_name,
            $delay,
            $group,
            $message
        );
    }

    /**
     * @param mixed $data
     *
     * @return int timestamp of the heartbeat
     */
    public function heartbeat($data = null)
    {
        if ($data !== null) {
            $this->data = $data;
            $jsonData   = json_encode($data, JSON_UNESCAPED_SLASHES);
        } else {
            $jsonData = null;
        }

        return $this->expires = $this->client->heartbeat($this->jid, $this->worker_name, $jsonData);
    }

    /**
     * Cancels the job and optionally all its dependents
     *
     * @param bool $dependents true if associated dependents should also be cancelled
     *
     * @return string[]
     */
    public function cancel($dependents = false)
    {
        if ($dependents && !empty($this->job_data['dependents'])) {
            return call_user_func_array([$this->client, 'cancel'], array_merge([$this->jid], $this->job_data['dependents']));
        }

        return $this->client->cancel($this->jid);
    }

    /**
     * Creates the instance to perform the job and calls the method on the Instance specified in the payload['performMethod'];
     */
    public function perform(): void
    {
        $instance = $this->getInstance();

        $performMethod = $this->data['performMethod'];

        $instance->$performMethod($this);
    }

    /**
     * @param string $group
     * @param string $message
     * @param mixed  $data
     *
     * @return string
     */
    public function fail($group, $message, $data = null)
    {
        if ($data !== null) {
            $this->data = $data;
            $jsonData   = json_encode($data, JSON_UNESCAPED_SLASHES);
        } else {
            $jsonData = null;
        }

        return $this->client->fail($this->jid, $this->worker_name, $group, $message, $jsonData);
    }

    /**
     * Timeout this job
     */
    public function timeout(): void
    {
        $this->client->timeout($this->jid);
    }

    /**
     * Get the instance of the class specified on this job.  This instance will
     * be used to call the payload['performMethod']
     *
     * @return mixed
     * @throws \Exception
     */
    public function getInstance()
    {
        if ($this->instance !== null) {
            return $this->instance;
        }

        if (!$this->klass_name) {
            throw new \Exception('Job class not specified.');
        }

        if (!is_array($this->data) || !isset($this->data['performMethod'])) {
            throw new \Exception('Perform method not specified.');
        }

        if (!class_exists($this->klass_name)) {
            throw new \Exception('Job class ' . $this->klass_name . ' not found.');
        }

        if (!method_exists($this->klass_name, $this->data['performMethod'])) {
            throw new \Exception('Perform method ' . $this->data['performMethod'] . ' not found in job class ' . $this->klass_name . '.');
        }

        return $this->instance = new $this->klass_name();
    }
}
