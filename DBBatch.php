<?
namespace mrssoft\db;

/**
 * Пакетная вставка / обновление записей
 */
class DbBatch extends \yii\base\Component
{
    private $data = [];

    /**
     * Добавить запись
     * @param array $data
     * @param mixed $key
     */
    public function add($data, $key = null)
    {
        if ($key === null) {
            $this->data[] = $data;
        } else {
            $this->data[$key] = $data;
        }
    }

    /**
     * Обновить запись
     * @param array $data
     * @param mixed $key
     */
    public function addToExist($data, $key)
    {
        if (array_key_exists($key, $this->data)) {
            $this->data[$key] = \yii\helpers\ArrayHelper::merge($this->data[$key], $data);
        }
    }

    /**
     * Удаление записи
     * @param mixed $key
     */
    public function remove($key)
    {
        if (array_key_exists($key, $this->data)) {
            unset($this->data[$key]);
        }
    }

    /**
     * Проверка наличия записи
     * @param $key
     * @return bool
     */
    public function exists($key)
    {
        return array_key_exists($key, $this->data);
    }

    public function getCount()
    {
        return count($this->data);
    }

    /**
     * Плучение значение элемента
     * @param $key
     * @return array|null
     */
    public function get($key)
    {
        if (array_key_exists($key, $this->data)) {
            return $this->data[$key];
        } else {
            return null;
        }
    }

    public function getData()
    {
        return array_values($this->data);
    }
	
    /**
     * Массив данных
     * @param array $array
     */
    public function setData($array = [])
    {
        $this->data = $array;
    }	

    public function getKeys()
    {
        return array_keys($this->data);
    }

    /**
     * Выполнить вставку
     * @return bool
     * @param $table
     * @param bool $truncate
     */
    public function insert($table, $truncate = false)
    {
        if ($truncate)
            \Yii::$app->db->createCommand()->truncateTable($table)->execute();

        return $this->execute('INSERT', $table);
    }

    /**
     * Выполнить замену
     * @return bool
     * @param $table
     * @param bool $truncate
     */
    public function replace($table, $truncate = false)
    {
        if ($truncate)
            \Yii::$app->db->createCommand()->truncateTable($table)->execute();

        return $this->execute('REPLACE', $table);
    }

    /**
     * Обновить запись
     * @param array $data
     * @param mixed $key
     */
    public function update($data, $key)
    {
        if (array_key_exists($key, $this->data))
        {
            $this->data[$key] = \yii\helpers\ArrayHelper::merge($this->data[$key], $data);
        }
    }

    /**
     * @param bool $value
     */
    public static function setForeignKey($value)
    {
        $value = (bool)$value ? '1' : '0';
        $sql = 'SET FOREIGN_KEY_CHECKS = ' . $value;
        \Yii::$app->db->createCommand($sql)->execute();
    }

    /**
     * Выполнить запрос
     * @param $table
     * @param $command
     * @return bool
     */
    private function execute($command, $table)
    {
        if (empty($this->data)) return false;
        if ($command == 'update') {
            $sql = $this->compileUpdate($table);
        } else {
            $sql = $this->compile($command, $table);
        }
        return \Yii::$app->db->createCommand($sql)->execute();
    }


    /**
     * Сформировать строку запроса
     * @param $command
     * @param $table
     * @return string
     */
    private function compile($command, $table)
    {
        $fields = [];
        foreach (array_keys(reset($this->data)) as $row) {
            $fields[] = '`' . $row . '`';
        }

        $values = [];
        foreach ($this->data as $row) {
            foreach ($row as &$v) {
                $v = \Yii::$app->db->getSlavePdo()->quote($v);
            }
            $values[] = '(' . implode(',', $row) . ')';
        }

        return $command . ' INTO ' . $table . ' (' . implode(',', $fields) . ') VALUES ' . implode(',', $values);
    }

    /**
     * @param $table
     */
    private function compileUpdate($table)
    {

    }
}