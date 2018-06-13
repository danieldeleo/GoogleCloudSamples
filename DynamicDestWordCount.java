package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.json.JSONArray;
import org.json.JSONObject;

public class DynamicDestWordCount {
  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it
   * to a ParDo in the pipeline.
   */
  static class ExtractWordsFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().split("[^\\p{L}]+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          word = word.toLowerCase();
          c.output(word);
        }
      }
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new DynamicDestWordCount.ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

  static class RemoveVeryLowCountWords
      extends DoFn<KV<String, Long>, KV<String, Long>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Long> wordCountMap = c.element();
      if(wordCountMap.getValue() > 500) {
        c.output(wordCountMap);
      }
    }
  }

  static class RemoveLowCountWords
      extends DoFn<KV<String, Long>, KV<String, Long>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Long> wordCountMap = c.element();
      if(wordCountMap.getValue() > 1000) {
        c.output(wordCountMap);
      }
    }
  }

  static class RemoveHighCountWords
      extends DoFn<KV<String, Long>, KV<String, Long>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Long> wordCountMap = c.element();
      if(wordCountMap.getValue() <= 1000 && wordCountMap.getValue() > 500) {
        c.output(wordCountMap);
      }
    }
  }

  /**
   * Options supported by {@link DynamicDestWordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface DynamicDestWordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/*")
    String getInputFile();
    void setInputFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    DynamicDestWordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(DynamicDestWordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    // final PCollectionView<List<String>> myschema =
    //     p.apply("ReadSchema",TextIO.read().from("gs://flowingmydata/schema.json"))
    //     .apply(View.<String>asList());

    PCollection<KV<String, Long>> wordCounts =
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply(new DynamicDestWordCount.CountWords())
            .apply(ParDo.of(new DynamicDestWordCount.RemoveVeryLowCountWords()));

    final PCollectionView<Map<String, Long>> lowCountWords =
        wordCounts.apply(ParDo.of(new DynamicDestWordCount.RemoveHighCountWords()))
            // .apply(Keys.<String>create())
            .apply(View.<String, Long>asMap());

    final PCollectionView<Map<String, Long>> highCountWords =
        wordCounts.apply(ParDo.of(new DynamicDestWordCount.RemoveLowCountWords()))
            // .apply(Keys.<String>create())
            .apply(View.<String, Long>asMap());

        wordCounts.apply(BigQueryIO.<KV<String, Long>>write()
            .to(new DynamicDestinations<KV<String, Long>, String>() {

              public String getDestination(ValueInSingleWindow<KV<String, Long>> wordCounts) {
                KV<String, Long> e = wordCounts.getValue();
                String word = e.getKey();
                String tableName;
                Map<String, Long> lowCountWordsMap = sideInput(lowCountWords);
                if(lowCountWordsMap.containsKey(word)) {
                  tableName = "Words_Counted_1000_Or_Less";
                } else {
                  tableName = "Words_Counted_More_Than_1000";
                }
                return tableName;
              }

              public TableDestination getTable(String tableName) {
                return new TableDestination("flowingmydata:flowndata." + tableName, "Table of " + tableName);
              }

              public TableSchema getSchema(String tableName) {
                // Build the table schema for the output table.
                Map<String, Long> lowCountWordsMap = sideInput(lowCountWords);
                Map<String, Long> highCountWordsMap = sideInput(highCountWords);
                List<TableFieldSchema> fields = new ArrayList<>();
                if(Objects.equals(tableName, "Words_Counted_1000_Or_Less")) {
                  for(String word : lowCountWordsMap.keySet()) {
                    fields.add(new TableFieldSchema().setName(word).setType("INTEGER"));
                  }
                } else {
                  for(String word : highCountWordsMap.keySet()) {
                    fields.add(new TableFieldSchema().setName(word).setType("INTEGER"));
                  }
                }
                return new TableSchema().setFields(fields);
              }

              public List<PCollectionView<?>> getSideInputs() {
                ArrayList<PCollectionView<?>> sideInputs = Lists.newArrayList();
                sideInputs.add(lowCountWords);
                sideInputs.add(highCountWords);
                return sideInputs;
              }

            })
            .withFormatFunction(new SerializableFunction<KV<String, Long>, TableRow>() {
              public TableRow apply(KV<String, Long> wordCounts) {
                String word = wordCounts.getKey();
                Long count = wordCounts.getValue();
                return new TableRow().set(word, count);
              }
            })//.withWriteDisposition(WriteDisposition.WRITE_APPEND)
        );
    p.run();
  }
}
