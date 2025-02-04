package cz.it4i.fiji.datastore.management;

import cz.it4i.fiji.datastore.security.Authorization;
import lombok.extern.log4j.Log4j2;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Mutation;

import javax.inject.Inject;

@GraphQLApi
@Authorization
@Log4j2
public class DataServerManagerResource {
    @Inject
    DataServerManager dataServerManager;

    @Mutation("stop")
    @Description("Stops a data server")
    public void stopDataServer() {
        log.debug("Stop was requested as REST request");
        dataServerManager.stopCurrentDataServer();
    }
}
