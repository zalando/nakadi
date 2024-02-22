package org.zalando.nakadi.plugin.auth.subject;

import org.json.JSONArray;
import org.json.JSONObject;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.OPAClient;
import org.zalando.nakadi.plugin.auth.ZalandoTeamService;

import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PrincipalFactory {

    private final String employeesRealm;
    private final String usersType;
    private final String servicesRealm;
    private final String servicesType;
    private final OPAClient opaClient;
    private final ZalandoTeamService teamService;

    public PrincipalFactory(
            final String employeesRealm,
            final String usersType,
            final String servicesRealm,
            final String servicesType,
            final OPAClient opaClient,
            final ZalandoTeamService teamService) {
        this.employeesRealm = employeesRealm;
        this.usersType = usersType;
        this.servicesRealm = servicesRealm;
        this.servicesType = servicesType;
        this.opaClient = opaClient;
        this.teamService = teamService;
    }

    public Principal createPrincipal(final String uid, final String consumer, final String realm)
            throws PluginException {
        final String type = convertRealmToType(realm);
        return buildSubject(uid, consumer, type);
    }

    private Principal buildSubject(final String uid, final String consumer, final String type) {
        final Supplier<Set<String>> retailerIdsSupplier = getRetailerIdsSupplier(uid, type);
        if (null != consumer) {
            try {
                final JSONObject bussinessPartnerJson = new JSONObject(
                        new String(Base64.getDecoder().decode(consumer)));
                final JSONArray bpidsArray = Optional.ofNullable(bussinessPartnerJson
                        .optJSONArray("bpids"))
                        .orElse(new JSONArray());
                final Set<String> bpids = bpidsArray.toList().stream().map(String::valueOf).collect(Collectors.toSet());
                return new ExternalSubject(uid, retailerIdsSupplier, bpids);
            } catch (Exception e) {
                throw new PluginException("Could not obtain valid business partner ids from the X-Consumer header", e);
            }
        } else {
            if (usersType.equals(type)) {
                return new EmployeeSubject(uid, retailerIdsSupplier, type, teamService);
            } else {
                return new UidSubject(uid, retailerIdsSupplier, type);
            }
        }
    }

    private String convertRealmToType(final String realm) throws PluginException {
        if (Objects.equals(employeesRealm, realm)) {
            return usersType;
        } else if (Objects.equals(servicesRealm, realm)) {
            return servicesType;
        } else {
            throw new PluginException("Token has unknown realm: " + realm);
        }
    }

    private Supplier<Set<String>> getRetailerIdsSupplier(final String uid, final String type) {
        if (Objects.equals(servicesType, type)) {
            return () -> opaClient.getRetailerIdsForService(uid);
        } else if (Objects.equals(usersType, type)) {
            return () -> opaClient.getRetailerIdsForUser(uid);
        } else {
            throw new PluginException("Token resolved into unknown type: " + type);
        }
    }
}
