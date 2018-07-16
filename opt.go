/**
 * gedis 操作
 * Created by pigsatwork on 07.12月.2017
 */
package opt

import (
	"errors"
	"gedis" //pigsatwork add
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/log4go"
)

//判断是否是重复上报同一个故障，如果是则不再推送，不是的话就进行推送
//返回值如果为true则为推送，false为不推送
func RepeatedFault(macField string, fault string) bool {

	fvalue, err := gedis.HGet(macField, fault)
	log4go.Debug("mac对应的错误%s值:%s", fault, fvalue)
	if err != nil {
		if err.Error() == "redis: nil" {
			if _, errs := gedis.HSet(macField, fault, "1"); errs != nil {
				log4go.Error("repeatedFault HSet error:", errs.Error())
				return false
			}
			log4go.Debug("Fault set in redis and redis:nil")

			return true
		} else {
			log4go.Error("repeatedFault error:", err.Error())
		}
	} else {
		if fvalue != "1" {
			if _, err := gedis.HSet(macField, fault, "1"); err != nil {
				log4go.Error("repeatedFault HSet error:", err.Error())
				return false
			}
			log4go.Debug("Fault set in redis and fvalue != 1")
			return true
		} else {
			log4go.Debug("Fault repeated")
			return false
		}
	}
	return false
}

////删除redis中的相关数据。对应为case5故障修复。返回true是成功删除，false是错误
func DelFault(macField string) (int64, bool) {
	delValue, errd := gedis.Del(macField) //delValue ==1时,说明删除了存在的数据.delValue ==0时,说明不存在要删除的数据
	if errd == nil {
		log4go.Debug("Fault Deleted")
		return delValue, true
	}
	log4go.Error("DelFault error")
	return delValue, false
}
