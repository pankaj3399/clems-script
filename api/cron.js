import { parse } from "node-html-parser";
import { join } from "path";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { parse as CSVParser } from "csv-parse/sync";
import dotenv from "dotenv";
import mongoose, { Schema, mongo } from "mongoose";

export const maxDuration = 60;

dotenv.config();

function getFormattedDate(d = new Date().toString()) {
  const date = new Date(d);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");

  return `${year}-${month}-${day}`;
}

const fileLogSchema = new Schema({
  id: Schema.ObjectId,
  name: {
    type: String,
    unique: true,
  },
  date: Date,
  url: String,
});

const FileLog = mongoose.model("filelogs", fileLogSchema);

const fileScehma = new Schema({
  id: Schema.ObjectId,
  name: {
    type: String,
    unique: true,
  },
  data: String,
  date: Date,
});

const File = mongoose.model("files", fileScehma);

// creating a mongo schema for today's csv data
const additionSchema = new Schema({
  id: Schema.ObjectId,
  name: String,
  townCity: String,
  county: String,
  type: String,
  route: String,
  date: String,
});

// assigning a model to mongodb user userSchema
const Addition = mongoose.model("addition", additionSchema);

// creating a mongo schema for today's csv data
const updateSchema = new Schema({
  id: Schema.ObjectId,
  name: String,
  townCity: String,
  county: String,
  type: String,
  route: String,
  date: String,
});

// assigning a model to mongodb user userSchema
const Updates = mongoose.model("updates", updateSchema);

// creating a mongo schema for today's csv data
const removalSchema = new Schema({
  id: Schema.ObjectId,
  name: String,
  townCity: String,
  county: String,
  type: String,
  route: String,
  date: String,
});

// assigning a model to mongodb user userSchema
const Removal = mongoose.model("removals", removalSchema);

export default async function handler() {
  const CSVFileDir = join(process.cwd(), "csv-files");

  if (!existsSync(CSVFileDir)) {
    mkdirSync(CSVFileDir);
  }

  await mongoose.connect(process.env.MONGO_CONNECTION);
  console.log("Connection Success:", mongoose.connection.name);

  const csvFileRes = await fetch(
    "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers",
    {
      headers: {
        "Content-Type": "text/html",
      },
    },
  );

  if (!csvFileRes.ok) {
    console.log("Current update file link fetch failed!");
    return;
  }
  const govUkCSVHtml = parse(await csvFileRes.text());

  const link = govUkCSVHtml.querySelector(".govuk-link.gem-c-attachment__link");
  const lastUpdateDate = govUkCSVHtml.querySelectorAll(
    ".app-c-published-dates__change-item time",
  )[1].innerText;

  // Sample Link
  // https://assets.publishing.service.gov.uk/media/6659951fd470e3279dd33443/2024-05-31_-_Worker_and_Temporary_Worker.csv
  // https://assets.publishing.service.gov.uk/media/665ed5eadc15efdddf1a86b6/2024-06-04_-_Worker_and_Temporary_Worker.csv
  const fileLink = link.getAttribute("href");
  const fileLinkArr = fileLink.replace("/preview", "").split("/");
  const fileName = fileLinkArr[fileLinkArr.length - 1];

  const currentUpdateFileLink = fileLink;
  const currentUpdateFileDate = fileName.split("_-_")[0];
  const lastUpdateFileDate = getFormattedDate(lastUpdateDate);

  console.log("LINK: ", fileLink);
  console.log("FILENAME: ", fileName);
  console.log("LAST_UPDATE_DATE: ", lastUpdateDate);
  console.log("LAST_UPDATE_DATE_FORMATTED: ", getFormattedDate(lastUpdateDate));

  const thisDaysFileNameCSV = `${currentUpdateFileDate}.csv`;
  const prevDaysFileNameCSV = `${lastUpdateFileDate}.csv`;
  console.log({ prevDaysFileNameCSV, thisDaysFileNameCSV });

  console.log(`Got Link: ${currentUpdateFileLink}`);
  console.log(`Today's Date: ${currentUpdateFileDate}`);

  console.log("Fetching File!");
  const res = await fetch(currentUpdateFileLink);

  if (!res.ok) {
    console.log("File Write Failed, Try Again!");
    return;
  }

  const currentUpdateCSVFile = await res.text();
  console.log("Downloded CSV File!");

  const newFileCheck = await File.findOne({
    name: currentUpdateFileDate,
  })
    .select("name")
    .exec();
  if (newFileCheck?.name === currentUpdateFileDate) {
    console.log(`
File with name ${currentUpdateFileDate} already saved to MongoDB
This could also mean that there is no update to CSV Source file 
since last script run.
Exiting Script!
`);
    return;
  } else {
    const newFileRecord = new File();
    newFileRecord.name = currentUpdateFileDate;
    newFileRecord.date = new Date();
    newFileRecord.data = currentUpdateCSVFile;
    await newFileRecord.save();

    console.log("File saved to MongoDB!:", `${currentUpdateFileDate}.csv`);
    // console.log("File: ", newFileRecord)
  }

  const currentUpdateCSVString = currentUpdateCSVFile;

  const lastUpdateFile = await File.findOne({
    name: lastUpdateFileDate,
  }).exec();
  console.log("File Downloded!: ", lastUpdateFile.name);

  if (!lastUpdateFile) {
    console.log(
      "Please add last updates file to MongoDB first, use prernu.js!",
    );
    return;
  }

  const lastUpdateCSVString = lastUpdateFile.data;

  const thisDateCSVDataParsed = CSVParser(currentUpdateCSVString, {
    columns: true,
  });
  const prevDateCSVDataParsed = CSVParser(lastUpdateCSVString, {
    columns: true,
  });

  console.log("thisDateCSVDataParsed", thisDateCSVDataParsed.length);
  console.log("prevDateCSVDataParsed", prevDateCSVDataParsed.length);

  const prevDayOrgObjectsByName = {};
  const thisDayOrgObjectsByName = {};
  const orgNameCol = "Organisation Name";

  prevDateCSVDataParsed.forEach((obj) => {
    prevDayOrgObjectsByName[obj[orgNameCol]] = obj;
  });
  thisDateCSVDataParsed.forEach((obj) => {
    thisDayOrgObjectsByName[obj[orgNameCol]] = obj;
  });

  console.log({
    prevDayOrgObjectsByName: Object.keys(prevDayOrgObjectsByName).length,
    thisDayOrgObjectsByName: Object.keys(thisDayOrgObjectsByName).length,
  });

  let prevDateNames = new Set();
  let thisDateNames = new Set();
  let prevDateNamesLowerCased = new Set();
  let thisDateNamesLowerCased = new Set();

  thisDateCSVDataParsed.map((row) => {
    if (row[orgNameCol]) {
      thisDateNames.add(row[orgNameCol]);
      thisDateNamesLowerCased.add(String(row[orgNameCol]).toLowerCase());
    }
  });
  prevDateCSVDataParsed.map((row) => {
    if (row[orgNameCol]) {
      prevDateNames.add(row[orgNameCol]);
      prevDateNamesLowerCased.add(String(row[orgNameCol]).toLowerCase());
    }
  });

  console.log({
    prevDateColNames: prevDateNames.size,
    thisDateColNames: thisDateNames.size,
  });

  const addedOrgNames = [...thisDateNames].filter(
    (x) => !prevDateNamesLowerCased.has(String(x).toLowerCase()),
  );
  const removedOrgNames = [...prevDateNames].filter(
    (x) => !thisDateNamesLowerCased.has(String(x).toLowerCase()),
  );

  console.log(
    `Values in ${lastUpdateFileDate} but not in ${currentUpdateFileDate}:`,
    removedOrgNames.length,
  );
  console.log(
    `Values in ${currentUpdateFileDate} but not in ${lastUpdateFileDate}:`,
    addedOrgNames.length,
  );

  const prevDateFullObjs = {};
  const thisDateFullObjs = {};

  addedOrgNames.forEach((orgName) => {
    thisDateFullObjs[orgName] = thisDayOrgObjectsByName[orgName];
  });
  removedOrgNames.forEach((orgName) => {
    prevDateFullObjs[orgName] = prevDayOrgObjectsByName[orgName];
  });

  console.log({
    prevDateFullObjs: Object.keys(prevDateFullObjs).length,
    thisDateFullObjs: Object.keys(thisDateFullObjs).length,
  });

  const addedPromises = addedOrgNames.map(async (row) => {
    const colNames = Object.keys(thisDateFullObjs[row]);
    const record = new Addition();
    record.name = thisDateFullObjs[row][colNames[0]];
    record.townCity = thisDateFullObjs[row][colNames[1]];
    record.county = thisDateFullObjs[row][colNames[2]];
    record.type = thisDateFullObjs[row][colNames[3]];
    record.route = thisDateFullObjs[row][colNames[4]];
    record.date = currentUpdateFileDate;
    await record.save();
  });
  await Promise.all(addedPromises);
  console.log("All Additions Added to DB!");

  const removedPromises = removedOrgNames.map(async (row) => {
    const colNames = Object.keys(prevDateFullObjs[row]);
    const record = new Removal();
    record.name = prevDateFullObjs[row][colNames[0]];
    record.townCity = prevDateFullObjs[row][colNames[1]];
    record.county = prevDateFullObjs[row][colNames[2]];
    record.type = prevDateFullObjs[row][colNames[3]];
    record.route = prevDateFullObjs[row][colNames[4]];
    record.date = currentUpdateFileDate;
    await record.save();
  });
  await Promise.all(removedPromises);
  console.log("All Removals Added to DB!");

  const updatedOrgsObjs = [];
  // const colToCheckForUpdates = "Town/City"
  const columnsToCheckForUpdates = ["Town/City", "County", "Type", "Route"];

  Object.keys(thisDayOrgObjectsByName).forEach((orgName) => {
    const thisDayObj = thisDayOrgObjectsByName[orgName];

    if (
      prevDateNamesLowerCased.has(String(orgName).toLowerCase()) &&
      !prevDateNames.has(orgName)
    ) {
      console.log("Updated: Name ", orgName, thisDayObj);
      updatedOrgsObjs.push(thisDayObj);
      return;
    }

    const prevDayObj = prevDayOrgObjectsByName[orgName];
    if (!prevDayObj) {
      return;
    }

    for (let i = 0; i < columnsToCheckForUpdates.length; i++) {
      const col = columnsToCheckForUpdates[i];
      if (prevDayObj[col] !== thisDayObj[col]) {
        console.log(`Updated: ${col}`, prevDayObj, thisDayObj);
        updatedOrgsObjs.push(thisDayObj);
        break;
      }
    }

    // console.log("Checking: ", orgName)
    // if (prevDayObj[colToCheckForUpdates] !== thisDayObj[colToCheckForUpdates]) {
    // 	updatedOrgsObjs.push(thisDayObj)
    // }
  });
  console.log("All Checked For Updates!");

  const updatePromises = updatedOrgsObjs.map(async (update) => {
    const colNames = Object.keys(update);
    const record = new Updates();
    record.name = update[colNames[0]];
    record.townCity = update[colNames[1]];
    record.county = update[colNames[2]];
    record.type = update[colNames[3]];
    record.route = update[colNames[4]];
    record.date = currentUpdateFileDate;
    await record.save();
  });
  await Promise.all(updatePromises);
  console.log("All Updates Added to DB!");

  const thisFileLogRecord = new FileLog();
  thisFileLogRecord.name = currentUpdateFileDate;
  thisFileLogRecord.date = new Date();
  await thisFileLogRecord.save();
  console.log("fileNameRecord", thisFileLogRecord);

  console.log("Fetching Old FIles to Delete!");
  const fileRecords = await File.find()
    .sort({ date: -1 })
    .allowDiskUse()
    .select("name")
    .skip(2)
    .exec();
  if (fileRecords?.length > 0) {
    const filesToDelPromises = fileRecords.map(async (file) => {
      await File.deleteOne({ name: file.name });
    });
    await Promise.all(filesToDelPromises);
    console.log("Deleted Files: ", fileRecords);
  } else {
    console.log("No File to Delete!");
    return;
  }
}
