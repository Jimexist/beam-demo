package me.jiayu.demobeam

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.TypeDescriptors

object Application {
    fun runBeam(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().create()
        val p = Pipeline.create(options)
        val lines = p.apply("readFile", TextIO.read().from("/Users/jiayu_liu/Desktop/aggregate_test_100.csv"))
        val words = lines.apply(
            "splitWords",
            FlatMapElements.into(TypeDescriptors.strings()).via(ProcessFunction { it.split(",") })
        )
        val wordLengths = words.apply(
            "getLength",
            MapElements.into(TypeDescriptors.integers())
                .via(ProcessFunction { it.length })
        )
        wordLengths
            .apply(ToString.elements())
            .apply(TextIO.write().to("/Users/jiayu_liu/Desktop/aggregate_test_100").withSuffix(".txt"))
        val state = p.run().waitUntilFinish()
        print(state)
    }
}

fun main(args: Array<String>) {
    Application.runBeam(args)
}