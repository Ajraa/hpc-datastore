package cz.it4i.fiji.datastore.register_service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.eclipse.microprofile.graphql.Type;

import java.net.URI;

@Type
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionParameters {
    URI uri;
    String uuid;
    long rX;
    long rY;
    long rZ;
    String version;
}
