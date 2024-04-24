package org.example

import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.io.File

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    var mongoClient: MongoClient? = null

    try {
        // Connectar-se a servidor de mongoDB
        val connectionString =
            "mongodb+srv://elkin:pepoClown123@sergioherrador.bwwhoy4.mongodb.net/?retryWrites=true&w=majority&appName=SergioHerrador"
        // Crear connexió
        mongoClient = MongoClients.create(connectionString)
        // Obtenir database SergioHerradorDiazLopez de la connexió a servidor
        val db = mongoClient.getDatabase("itb")
        // Obtenir col·lecció  grades de la database
        val coll: MongoCollection<Document> = db.getCollection("productes")
        val collRestaurants: MongoCollection<Document> = db.getCollection("restaurants")

        afegirProductes(coll)
        afegirRestaurants(collRestaurants)
        // Control de errors.
    } catch (e: MongoTimeoutException) {
        println("No arribem a la base de dades")
    } catch (e: MongoException) {
        println(e.message)
    } catch (e: Exception) {
        println(e.message)
    } finally {
        // Cerrar la conexión de MongoDB al finalizar
        mongoClient?.close()
    }
}

// Afegim els productes del document products.json a la db
fun afegirProductes(coll: MongoCollection<Document>) {
    // Lectura del archivo JSON
    val jsonFile = File("src/main/JSON/products.json")
    println(jsonFile.absolutePath)

    // Creem llista buida i un contador
    var jsonDocuments: MutableList<String> = mutableListOf()
    var nRegistros = 0

    // Per cada linea del fitxer json la llegim i la afegim a la llista d'abans.
    // Y cada 1000 registres fem una pujada al la db per evitar sobrecarregar la memoria.
    jsonFile.forEachLine {
        jsonDocuments.add(it)

        nRegistros++

        if (nRegistros == 1000) {
            coll.insertMany(jsonDocuments.map { Document.parse(it) })
            nRegistros = 0
            jsonDocuments = mutableListOf()
        }
    }

    // Fem una ultima pujada per asegurar que no queda cap registre sense pujar.
    coll.insertMany(jsonDocuments.map { Document.parse(it) })
}


// Afegim els productes del document restaurants.json a la db
fun afegirRestaurants(coll: MongoCollection<Document>) {
    // Lectura del archivo JSON
    val jsonFile = File("src/main/JSON/restaurants.json")
    println(jsonFile.absolutePath)

    // Creem llista buida i un contador
    var jsonDocuments: MutableList<String> = mutableListOf()
    var nRegistros = 0

    // Per cada linea del fitxer json la llegim i la afegim a la llista d'abans.
    // Y cada 1000 registres fem una pujada al la db per evitar sobrecarregar la memoria.
    jsonFile.forEachLine {
        jsonDocuments.add(it)

        nRegistros++

        if (nRegistros == 1000) {
            coll.insertMany(jsonDocuments.map { Document.parse(it) })
            nRegistros = 0
            jsonDocuments = mutableListOf()
        }
    }

    // Fem una ultima pujada per asegurar que no queda cap registre sense pujar.
    coll.insertMany(jsonDocuments.map { Document.parse(it) })
}