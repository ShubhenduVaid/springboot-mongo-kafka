package com.example.demo;

import com.example.demo.models.Pets;
import com.example.demo.repositories.PetsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

import org.bson.types.ObjectId;

@RestController
@RequestMapping("/pets")
public class PetsController {

    @Value(value = "${test.topic.name}")
    private String testTopicName;

    @Value(value = "${test1.topic.name}")
    private String test1TopicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Pets> petsKafkaTemplate;

    @KafkaListener(topics = "test")
    public void listen(String message) {
        System.out.println("Recieved message String : " + message);
    }

    @KafkaListener(
            topics = "test1",
            containerFactory = "petsKafkaListenerContainerFactory"
    )
    public void petsListener(Pets pets) {
        System.out.println("Recieved message JSON : " + pets.toString());
    }

    @Autowired
    private PetsRepository repository;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public List<Pets> getAllPets() {
        // test //
        kafkaTemplate.send(testTopicName, "Hello");
        Pets pets = new Pets();
        pets.set_id(ObjectId.get());
        pets.setBreed("testBreed");
        pets.setName("testName");
        pets.setSpecies("testSpecies");
        petsKafkaTemplate.send(test1TopicName, pets);
        //////////
        return repository.findAll();
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Pets getPetById(@PathVariable("id") ObjectId id) {
        return repository.findBy_id(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public void modifyPetById(@PathVariable("id") ObjectId id, @Valid @RequestBody Pets pets) {
        pets.set_id(id);
        repository.save(pets);
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    public Pets createPet(@Valid @RequestBody Pets pets) {
        pets.set_id(ObjectId.get());
        repository.save(pets);
        return pets;
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deletePet(@PathVariable ObjectId id) {
        repository.delete(repository.findBy_id(id));
    }
}