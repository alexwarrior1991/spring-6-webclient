package guru.springframework.client;

import com.fasterxml.jackson.databind.JsonNode;
import guru.springframework.exception.ClientException;
import guru.springframework.exception.ResourceNotFoundException;
import guru.springframework.model.BeerDTO;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.rmi.ServerException;
import java.time.Duration;
import java.util.Map;

@Service
public class BeerClientImpl implements BeerClient {

    public static final String BEER_PATH = "/api/v3/beer";
    public static final String BEER_PATH_ID = BEER_PATH + "/{beerId}";
    private final WebClient webClient;

    public BeerClientImpl(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    @Override
    public Mono<Void> deleteBeer(BeerDTO dto) {
        return webClient.delete()
                .uri(
                        uriBuilder -> uriBuilder.path(BEER_PATH_ID).build(dto.getId())
                )
                .retrieve()
                .toBodilessEntity()
                .then();
    }

    @Override
    public Mono<BeerDTO> patchBeer(BeerDTO beerDTO) {
        return webClient.patch()
                .uri(
                        uriBuilder -> uriBuilder.path(BEER_PATH_ID).build(beerDTO.getId())
                )
                .body(Mono.just(beerDTO), BeerDTO.class)
                .retrieve()
                .toBodilessEntity()
                .flatMap(
                        voidResponseEntity -> getBeerById(beerDTO.getId())
                );
    }

    @Override
    public Mono<BeerDTO> updateBeer(BeerDTO beerDTO) {
        return webClient.put()
                .uri(
                        uriBuilder -> uriBuilder.path(BEER_PATH_ID).build(beerDTO.getId())
                )
                .body(Mono.just(beerDTO), BeerDTO.class)
                .retrieve()
                .toBodilessEntity()
                .flatMap(
                        voidResponseEntity -> getBeerById(beerDTO.getId())
                );
    }

    @Override
    public Mono<BeerDTO> createBeer(BeerDTO beerDTO) {
        return webClient.post()
                .uri(BEER_PATH)
                .body(Mono.just(beerDTO), BeerDTO.class)
                .retrieve()
                .toBodilessEntity()
                .flatMap(
                        voidResponseEntity -> Mono.just(voidResponseEntity
                                .getHeaders().get("Location").get(0))
                )
                .map(path -> path.split("/")[path.split("/").length - 1])
                .flatMap(this::getBeerById);
    }

    @Override
    public Flux<BeerDTO> getBeerByBeerStyle(String beerStyle) {
        return webClient.get()
                .uri(
                        uriBuilder -> uriBuilder
                                .path(BEER_PATH)
                                .queryParam("beerStyle", beerStyle).build()
                )
                .retrieve()
                .bodyToFlux(BeerDTO.class);
    }

    @Override
    public Mono<BeerDTO> getBeerById(String id) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder.path(BEER_PATH_ID)
                        .build(id)
                )
                .retrieve()
                .bodyToMono(BeerDTO.class);
    }

    @Override
    public Mono<BeerDTO> findBeerById(String id) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder.path(BEER_PATH_ID)
                        .build(id)
                )
                .exchangeToMono(response -> {
                    // Log the response status
                    System.out.println("Response status: " + response.statusCode());

                    // Access and log headers
                    HttpHeaders headers = response.headers().asHttpHeaders();
                    System.out.println("Content-Type: " + headers.getContentType());


                    if (response.statusCode().is2xxSuccessful()) {
                        return response.bodyToMono(BeerDTO.class)
                                .doOnNext(beer -> System.out.println("Successfully retrieved beer: " + beer.getBeerName()));
                    } else if (response.statusCode().equals(HttpStatus.NOT_FOUND)) {
                        return Mono.error(new ResourceNotFoundException("Beer not found with ID: " + id));
                    } else if (response.statusCode().is4xxClientError()) {
                        return Mono.error(new ClientException("Client error: " + response.statusCode()));
                    } else if (response.statusCode().is5xxServerError()) {
                        // Implement retry logic for server errors
                        return Mono.error(new ServerException("Server error: " + response.statusCode()));
                    } else {
                        return response.createException()
                                .flatMap(Mono::error);
                    }
                })
                .retry(3)
                .timeout(Duration.ofSeconds(5))
                .doOnError(error -> System.err.println("Error retrieving beer: " + error.getMessage()));

    }

    @Override
    public Flux<BeerDTO> listBeerDtos() {
        return webClient.get().uri(BEER_PATH)
                .retrieve().bodyToFlux(BeerDTO.class);
    }

    @Override
    public Flux<JsonNode> listBeersJsonNode() {
        return webClient.get().uri(BEER_PATH)
                .retrieve().bodyToFlux(JsonNode.class);
    }

    @Override
    public Flux<Map> listBeerMap() {
        return webClient.get().uri(BEER_PATH)
                .retrieve().bodyToFlux(Map.class);
    }

    @Override
    public Flux<String> listBeer() {
        return webClient.get().uri(BEER_PATH)
                .retrieve().bodyToFlux(String.class);
    }


}
