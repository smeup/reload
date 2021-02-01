package com.smeup.dbnative.log

import org.slf4j.Logger
import org.slf4j.LoggerFactory


interface Logging

inline fun <reified T : Logging> T.logger(): Logger = getLogger(T::class.java)
fun getLogger(forClass: Class<*>): Logger = LoggerFactory.getLogger(forClass)

