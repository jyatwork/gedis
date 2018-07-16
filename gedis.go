package gedis

import (
	"fmt"
	"time"

	"github.com/alecthomas/log4go"
	"github.com/go-redis/redis"
)

var redisClient *redis.Client

func NewRedisClient(host, pwd string, port, db, poolSize int) error {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: pwd,
		DB:       db,
		PoolSize: poolSize,
	})

	if _, err := client.Ping().Result(); err != nil {
		log4go.Error("Database ping:", err.Error())
		return err
	}

	redisClient = client
	return nil
}

func Set(key, value string) (string, error) {
	statCmd := redisClient.Set(key, value, 0)
	res, err := statCmd.Result()
	if err != nil {
		log4go.Error("Database: redis Set: ", err.Error())
		return "", err
	}

	return res, nil
}

func SetWithExpir(key, value string, expir time.Duration) (string, error) {
	statCmd := redisClient.Set(key, value, expir)
	res, err := statCmd.Result()
	if err != nil {
		log4go.Error("Database: redis SetWithExpir: ", err.Error())
		return "", err
	}

	return res, nil
}

func Exist(key string) (int64, error) {
	boolCmd := redisClient.Exists(key)
	if boolCmd.Err() != nil {
		log4go.Error("Database: redis HFieldExist: ", boolCmd.Err())
		return 0, boolCmd.Err()
	}
	return boolCmd.Val(), nil
}

func Get(key string) (string, error) {
	strCmd := redisClient.Get(key)
	if strCmd.Err() != nil {
		if strCmd.Err().Error() != "redis: nil" {
			log4go.Error("Database: redis Get: ", strCmd.Err())
		}

		return "", strCmd.Err()
	}
	return strCmd.Val(), nil
}

func HSet(key, field, value string) (bool, error) {
	boolCmd := redisClient.HSet(key, field, value)
	if boolCmd.Err() != nil {
		log4go.Error("Database: redis HSet: ", boolCmd.Err())
		return false, boolCmd.Err()
	}
	return boolCmd.Val(), nil
}

func HGet(key, field string) (string, error) {
	strCmd := redisClient.HGet(key, field)
	if strCmd.Err() != nil {
		if strCmd.Err().Error() != "redis: nil" {
			log4go.Error("Database: redis HGet %s", strCmd.Err())
		}

		return "", strCmd.Err()
	}
	return strCmd.Val(), nil
}

func HMSet(key string, fields map[string]interface{}) (string, error) {
	statCmd := redisClient.HMSet(key, fields)
	if statCmd.Err() != nil {
		log4go.Error("Database: redis HMSet: ", statCmd.Err())
		return "", statCmd.Err()
	}
	return statCmd.Val(), nil
}

func HFieldExist(key, field string) (bool, error) {
	boolCmd := redisClient.HExists(key, field)
	if boolCmd.Err() != nil {
		log4go.Error("Database: redis HFieldExist: ", boolCmd.Err())
		return false, boolCmd.Err()
	}
	return boolCmd.Val(), nil
}
func Close() {
	redisClient.Close()
}

//del
func Del(key string) (int64, error) {
	boolCmd := redisClient.Del(key)
	if boolCmd.Err() != nil {
		log4go.Error("Database: redis KeyDel: ", boolCmd.Err())
		return 0, boolCmd.Err()
	}
	return boolCmd.Val(), nil
}

//Hdel
func Hdel(key, field string) (int64, error) {
	intCmd := redisClient.HDel(key, field)
	if intCmd.Err() != nil {
		log4go.Error("Database: redis KeyDel: ", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Val(), nil
}
