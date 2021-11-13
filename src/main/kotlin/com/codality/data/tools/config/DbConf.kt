package com.codality.data.tools.config

data class DbConf (
    val mongo: MongoConf? = null,
    val redis: RedisConf? = null
)