import mongoose, { Schema, mongo } from "mongoose"
import { parse } from "node-html-parser"
import { existsSync, mkdirSync, createReadStream, readFileSync } from "fs"
import { join } from "path"
import dotenv from "dotenv"
import Grid from "gridfs-stream"
import mongodb from "mongodb"

dotenv.config()

const fileNamesSchema = new Schema({
	id: Schema.ObjectId,
	name: String,
	date: Date,
	url: String,
})

const FileName = mongoose.model("fileName", fileNamesSchema)

const fileScehma = new Schema({
	id: Schema.ObjectId,
	name: {
		type: String,
		unique: true,
	},
	data: String,
	date: Date,
})

const File = mongoose.model("files", fileScehma)

export default async function prerun() {
	const fileName = "2024-06-10"
	const fileNamePath = join(process.cwd(), "csv-files", `${fileName}.csv`)

	await mongoose.connect(process.env.MONGO_CONNECTION)
	console.log("Connection Success:", mongoose.connection.name)

	const fileContents = readFileSync(fileNamePath)
	console.log("Reading File: ", fileName)

	const newFileRecord = new File()
	newFileRecord.name = fileName
	newFileRecord.date = new Date()
	newFileRecord.data = fileContents
	await newFileRecord.save()

	console.log("File Write Success:", `${fileName}.csv`)
	console.log("File: ", newFileRecord)
}

prerun()
	.then((res) => {
		console.log(res)
		mongoose.disconnect()
	})
	.catch((e) => {
		console.log("Error:", e)
		mongoose.disconnect()
	})
