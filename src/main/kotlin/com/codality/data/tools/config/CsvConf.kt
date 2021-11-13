package com.codality.data.tools.config

data class CsvConf (
    val regex: String? = null,
    val fields: List<CsvField>? = null,
    val hasHeader: Boolean? = null,
)
class CsvField (
    val sourceColumn: Int? = null,
    val targetColumn: Int? = null,
    val name: String? = null
)