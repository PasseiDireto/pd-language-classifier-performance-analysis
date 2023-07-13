import "dotenv/config";
import fs from "node:fs";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import csv from "csvtojson";
import LanguageDetect from "languagedetect";

const MAX_TEXT_LENGTH = 24000;
const BUCKET = process.env.BUCKET;

const fileData = {
  es: {
    materials: "materiais-studenta-marcados-como-pt.csv",
    currentLanguage: "pt",
    expectedLanguage: "es",
    output: "results/results-materiais-studenta-marcados-como-pt.json",
    aggregatedResultsFolder: "aggregated-results/materiais-studenta",
  },
  pt: {
    materials: "materiais-pd-marcados-como-es.csv",
    currentLanguage: "es",
    expectedLanguage: "pt",
    output: "results/results-materiais-pd-marcados-como-es.json",
    aggregatedResultsFolder: "aggregated-results/materiais-pd",
  },
};

const client = new S3Client({
  region: process.env.REGION,
});

const langdetect = new LanguageDetect();

const getTextPreview = async (fileFingerprint) => {
  let page = 1;
  let textPreview = "";

  while (textPreview.length < MAX_TEXT_LENGTH) {
    try {
      const command = new GetObjectCommand({
        Bucket: BUCKET,
        Key: `TextPreview/${fileFingerprint}/${page}.txt`,
      });

      const response = await client.send(command);
      const str = (await response.Body.transformToString()) || "";
      textPreview += str
        .replaceAll(/ +/g, " ")
        .replaceAll(/(^ +| +$)/g, "")
        .replaceAll(/(\r?\n?\t?)+/g, "")
        .replaceAll(/https?.*?(?= |$)/g, "");

      page++;
    } catch (error) {
      if (error.Code === "NoSuchKey") {
        break;
      } else {
        throw error;
      }
    }
  }

  return textPreview.slice(0, MAX_TEXT_LENGTH);
};

const detectLanguage = async (textPreview) => {
  const body = { text: textPreview, count: 1 };

  const response = await fetch("http://localhost:8064/language-detection", {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });

  const data = await response.json();

  const { language } = data;

  return {
    new: language[0].codex,
    old: langdetect.detect(textPreview, 1)?.[0]?.[0] || "",
  };
};

const aggregateResults = (results, outputFolder) => {
  const newResults = results.map(
    ({
      id,
      name,
      newDetectedLanguage,
      oldDetectedLanguage,
      currentLanguage,
      expectedLanguage,
    }) => ({
      id,
      name,
      currentLanguage,
      expectedLanguage,
      newDetectedLanguage,
      oldDetectedLanguage,
    })
  );

  const differentResults = newResults.filter((result) => {
    return (
      result.expectedLanguage !== result.newDetectedLanguage &&
      result.currentLanguage !== result.newDetectedLanguage
    );
  });

  const sameAsCurrent = newResults.filter((result) => {
    return result.currentLanguage === result.newDetectedLanguage;
  });

  const sameAsExpected = newResults.filter((result) => {
    return result.expectedLanguage === result.newDetectedLanguage;
  });

  fs.writeFileSync(
    `${outputFolder}/different-than-expected-and-current.json`,
    JSON.stringify(differentResults)
  );
  fs.writeFileSync(
    `${outputFolder}/same-as-current.json`,
    JSON.stringify(sameAsCurrent)
  );
  fs.writeFileSync(
    `${outputFolder}/same-as-expected.json`,
    JSON.stringify(sameAsExpected)
  );

  return {
    differentResults,
    sameAsCurrent,
    sameAsExpected,
  };
};

const getSample = (results) => {
  const sampleSize = 25;
  const shuffled = results.sort(() => 0.5 - Math.random());

  return shuffled.slice(0, sampleSize);
};

const getSamples = (
  differentResults,
  sameAsCurrent,
  sameAsExpected,
  outputFolder
) => {
  const differentResultsSample = getSample(differentResults);
  const sameAsCurrentSample = getSample(sameAsCurrent);
  const sameAsExpectedSample = getSample(sameAsExpected);

  fs.writeFileSync(
    `${outputFolder}/manual-analysis/different-than-expected-and-current-sample.json`,
    JSON.stringify(differentResultsSample)
  );
  fs.writeFileSync(
    `${outputFolder}/manual-analysis/same-as-current-sample.json`,
    JSON.stringify(sameAsCurrentSample)
  );
  fs.writeFileSync(
    `${outputFolder}/manual-analysis/same-as-expected-sample.json`,
    JSON.stringify(sameAsExpectedSample)
  );
};

const runJob = async (key) => {
  const {
    materials,
    currentLanguage,
    expectedLanguage,
    output,
    aggregatedResultsFolder,
  } = fileData[key];

  console.log("Reading file...");
  const files = await csv().fromFile(materials);

  console.log("Fetching results...");
  const results = await Promise.all(
    files.map(async (file) => {
      const { id, fileurl, name } = file;
      const textPreview = await getTextPreview(fileurl);

      const results = await detectLanguage(textPreview);
      const { new: language, old: oldLanguage } = results;

      return {
        id,
        name,
        fileurl,
        currentLanguage,
        expectedLanguage,
        newDetectedLanguage: language,
        oldDetectedLanguage: oldLanguage,
        textPreviewLength: textPreview.length,
        textPreview,
      };
    })
  );

  console.log("Writing results...");
  fs.writeFileSync(output, JSON.stringify(results));

  console.log("Analysing results for key: ", key, "...");
  const { differentResults, sameAsCurrent, sameAsExpected } = aggregateResults(
    results,
    aggregatedResultsFolder
  );

  console.log("Different than expected and current: ", differentResults.length);
  console.log("Same as current: ", sameAsCurrent.length);
  console.log("Same as expected: ", sameAsExpected.length);
  console.log("Total: ", results.length);

  console.log("Getting sample for manual analysis...");
  getSamples(
    differentResults,
    sameAsCurrent,
    sameAsExpected,
    aggregatedResultsFolder
  );

  console.log("Done!");
};

const countManualAnalysisResults = () => {
  const folders = [
    "aggregated-results/materiais-studenta",
    "aggregated-results/materiais-pd",
  ];

  const result = {
    newMethod: {
      correct: 0,
      incorrect: 0,
    },
    oldMethod: {
      correct: 0,
      incorrect: 0,
    },
  };

  folders.forEach((folder) => {
    const differentThanExpectedAndCurrent = JSON.parse(
      fs.readFileSync(
        `${folder}/manual-analysis/different-than-expected-and-current-sample.json`
      )
    );
    const sameAsCurrent = JSON.parse(
      fs.readFileSync(`${folder}/manual-analysis/same-as-current-sample.json`)
    );
    const sameAsExpected = JSON.parse(
      fs.readFileSync(`${folder}/manual-analysis/same-as-expected-sample.json`)
    );

    const totalAnalysis = [
      ...differentThanExpectedAndCurrent,
      ...sameAsCurrent,
      ...sameAsExpected,
    ];

    for (const item of totalAnalysis) {
      const { newAnalysis, oldAnalysis } = item;

      if (newAnalysis === "Correto") {
        result.new.correct++;
      } else {
        result.new.incorrect++;
      }

      if (oldAnalysis === "Correto") {
        result.old.correct++;
      } else {
        result.old.incorrect++;
      }
    }
  });

  console.log({ result });
};

runJob("pt");
runJob("es");
// countManualAnalysisResults();
