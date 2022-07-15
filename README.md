# Moesif Play Filter SDK

 [![Built For][ico-built-for]][link-built-for]
 [![Latest Version][ico-version]][link-package]
 [![Software License][ico-license]][link-license]
 [![Source Code][ico-source]][link-source]

## Introduction

`moesif-play-filter` is a Play framework Filter that logs API calls and sends to [Moesif](https://www.moesif.com) for API analytics and monitoring.

The SDK is implemented as a [Scala HTTP Filter](https://www.playframework.com/documentation/latest/ScalaHttpFilters)
without importing framework specific dependencies. Any Web Application built on Play can use this SDK with minimal configuration.

[Source Code on GitHub](https://github.com/moesif/moesif-play-filter)

## How to install

For SBT users, add dependency to your `build.sbt`:

```bash
libraryDependencies += "com.moesif.filter" % "moesif-play-filter" % "1.15.0"
```

For Maven users, add dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.moesif.filter</groupId>
    <artifactId>moesif-play-filter</artifactId>
    <version>1.15.0</version>
</dependency>
```

For Gradle users, add the Moesif dependency to your project's build.gradle file:

```gradle
dependencies {   
    compile 'com.moesif.filter:moesif-play-filter:1.15.0'
}
```

#### Others

The jars are available from a public [repo.maven.apache.org - moesif-play-filter](https://repo.maven.apache.org/maven2/com/moesif/filter/moesif-play-filter/) repository.


## How to use

MoesifApiFilter can be enabled using play framework application configuration. More details [Default HTTP filters](https://www.playframework.com/documentation/latest/Filters) and [Scala Http filters](https://www.playframework.com/documentation/latest/ScalaHttpFilters) 

In your play configuration file(default application.conf), enable MoesifApiFilter as below

```
play.filters.enabled += "com.moesif.filter.MoesifApiFilter"

play.modules.enabled += "com.moesif.filter.MoesifApiFilterModule"

play.filters.moesifApiFilter.moesifApplicationId = "Your Moesif application Id"
```

Your Moesif Application Id can be found in the [_Moesif Portal_](https://www.moesif.com/).
After signing up for a Moesif account, your Moesif Application Id will be displayed during the onboarding steps. 

You can always find your Moesif Application Id at any time by logging 
into the [_Moesif Portal_](https://www.moesif.com/), click on the top right menu,
and then clicking _Installation_.

With the above basic configuration setup Moesif filter will capture api events and send them to Moesif. 

## Advanced Configuration options

Moesif provides the most value when we can identify users. You may also want to specify metadata, mask certain data, or prevent tracking of certain requests entirely. This is possible with the hooks available in trait MoesifAdvancedFilterConfiguration.

To configure the filter, extend the `MoesifAdvancedFilterConfiguration` class to override methods as desired and then
set config in MoesifAdvancedFilterConfiguration to override the default config

```scala
import com.moesif.filter.MoesifAdvancedFilterConfiguration
import play.api.mvc.{RequestHeader, Result}

class CustomMoesifAdvancedConfig extends MoesifAdvancedFilterConfiguration {

  override def getMetadata(request: RequestHeader, result: Result): Map[String, Object] = Map("customAttribute" -> "atrributeValue")
}

// In you root play application initialization code set custom config
MoesifAdvancedFilterConfiguration.setConfig(new CustomMoesifAdvancedConfig())
```
Advanced configuration options are explained beloe

#### 1. `def skip(request: RequestHeader, result: Result): Boolean`
Return `true` if you want to skip logging a
request to Moesif i.e. to skip boring requests like health probes.

```scala
  override def skip(request: RequestHeader, result: Result): Boolean =  {
    // Skip logging health probes
    request.uri == "health/probe";
  }
```

#### 2. `def maskContent(moesifEventModel: EventModel): EventModel = moesifEventModel`
If you want to remove any sensitive data in the HTTP headers or body before sending to Moesif, you can do so with `maskContent`

#### 3. `def identifyUser(request: RequestHeader, result: Result): Option[String]`
Highly recommended. Even though Moesif automatically detects the end userId if possible, setting this configuration
ensures the highest accuracy with user attribution.

```scala
  override def identifyUser(request: RequestHeader, result: Result): Option[String] = {
    request.headers.headers.find(_._1 == "X-User-Id").map(_._2)
  }
```

#### 4. `def identifyCompany(request: RequestHeader, result: Result): Option[String]`
You can set this configuration to add company Id to the event.

```scala
  override def identifyCompany(request: RequestHeader, result: Result): Option[String] = {
    request.headers.headers.find(_._1 == "X-Company-Id").map(_._2)
  }
```

#### 5. `def sessionToken(request: RequestHeader, result: Result): Option[String]`

Moesif automatically detects the end user's session token or API key, but you can manually define the token for finer control.

```scala
  override def sessionToken(request: RequestHeader, result: Result): Option[String] = {
    request.headers.headers.toMap.get("Authorization")
  }
```

#### 6. `def getMetadata(request: RequestHeader, result: Result): Map[String, Object] `
You can add any additional tags as needed
to the event. Ensure that returned metadata map is Json serializable

#### 7. `debug`
(optional) boolean, a flag to see debugging messages.

**The below methods to update user and company are accessible via the Moesif Java API lib which Moesif Play Filter already imports as a dependency.**

## Update a Single User

Create or update a user profile in Moesif. 
The metadata field can be any customer demographic or other info you want to store.
Only the `user_id` field is required.
For details, visit the [Java API Reference](https://www.moesif.com/docs/api?java#update-a-user).

```java
MoesifFilter filter = new MoesifFilter("Your Moesif Application Id", new MoesifConfiguration());

// Campaign object is optional, but useful if you want to track ROI of acquisition channels
// See https://www.moesif.com/docs/api#users for campaign schema
CampaignModel campaign = new CampaignBuilder()
        .utmSource("google")
        .utmCampaign("cpc")
        .utmMedium("adwords")
        .utmTerm("api+tooling")
        .utmContent("landing")
        .build();

// Only userId is required
// metadata can be any custom object
UserModel user = new UserBuilder()
    .userId("12345")
    .companyId("67890") // If set, associate user with a company object
    .campaign(campaign)
    .metadata(APIHelper.deserialize("{" +
        "\"email\": \"johndoe@acmeinc.com\"," +
        "\"first_name\": \"John\"," +
        "\"last_name\": \"Doe\"," +
        "\"title\": \"Software Engineer\"," +
        "\"sales_info\": {" +
            "\"stage\": \"Customer\"," +
            "\"lifetime_value\": 24000," +
            "\"account_owner\": \"mary@contoso.com\"" +
          "}" +
        "}"))
    .build();

filter.updateUser(user);
```

## Update Users in Batch

Similar to UpdateUser, but used to update a list of users in one batch. 
Only the `user_id` field is required.
For details, visit the [Java API Reference](https://www.moesif.com/docs/api?java#update-users-in-batch).

```java
MoesifFilter filter = new MoesifFilter("Your Moesif Application Id", new MoesifConfiguration());
List<UserModel> users = new ArrayList<UserModel>();

// Only userId is required
// metadata can be any custom object
UserModel userA = new UserBuilder()
    .userId("12345")
    .companyId("67890") // If set, associate user with a company object
    .metadata(APIHelper.deserialize("{" +
        "\"email\": \"johndoe@acmeinc.com\"," +
        "\"first_name\": \"John\"," +
        "\"last_name\": \"Doe\"," +
        "\"title\": \"Software Engineer\"," +
        "\"sales_info\": {" +
            "\"stage\": \"Customer\"," +
            "\"lifetime_value\": 24000," +
            "\"account_owner\": \"mary@contoso.com\"" +
          "}" +
        "}"))
    .build();

// Only userId is required
// metadata can be any custom object
UserModel userB = new UserBuilder()
    .userId("54321")
    .companyId("67890") // If set, associate user with a company object
    .metadata(APIHelper.deserialize("{" +
        "\"email\": \"mary@acmeinc.com\"," +
        "\"first_name\": \"Mary\"," +
        "\"last_name\": \"Jane\"," +
        "\"title\": \"Software Engineer\"," +
        "\"sales_info\": {" +
            "\"stage\": \"Customer\"," +
            "\"lifetime_value\": 48000," +
            "\"account_owner\": \"mary@contoso.com\"" +
          "}" +
        "}"))
    .build();

users.add(userA);
users.add(userB);

filter.updateUsersBatch(users);
```

## Update a Single Company

Update user is accessible via the Moesif Java API lib which Moesif Play Filter already imports
as a dependency.

Create or update a company profile in Moesif.
The metadata field can be any company demographic or other info you want to store.
Only the `company_id` field is required.
For details, visit the [Java API Reference](https://www.moesif.com/docs/api?java#update-a-company).

```java
MoesifFilter filter = new MoesifFilter("Your Moesif Application Id", new MoesifConfiguration());

// Campaign object is optional, but useful if you want to track ROI of acquisition channels
// See https://www.moesif.com/docs/api#update-a-company for campaign schema
CampaignModel campaign = new CampaignBuilder()
        .utmSource("google")
        .utmCampaign("cpc")
        .utmMedium("adwords")
        .utmTerm("api+tooling")
        .utmContent("landing")
        .build();

// Only companyId is required
// metadata can be any custom object
CompanyModel company = new CompanyBuilder()
    .companyId("67890")
    .companyDomain("acmeinc.com") // If set, Moesif will enrich your profiles with publicly available info 
    .campaign(campaign) 
    .metadata(APIHelper.deserialize("{" +
        "\"org_name\": \"Acme, Inc\"," +
        "\"plan_name\": \"Free\"," +
        "\"deal_stage\": \"Lead\"," +
        "\"mrr\": 24000," +
        "\"demographics\": {" +
            "\"alexa_ranking\": 500000," +
            "\"employee_count\": 47" +
          "}" +
        "}"))
    .build();

filter.updateCompany(company);
```

## Update Companies in Batch

Similar to UpdateCompany, but used to update a list of companies in one batch. 
Only the `company_id` field is required.
For details, visit the [Java API Reference](https://www.moesif.com/docs/api?java#update-companies-in-batch).

```java
MoesifFilter filter = new MoesifFilter("Your Moesif Application Id", new MoesifConfiguration());
List<CompanyModel> companies = new ArrayList<CompanyModel>();

// Only companyId is required
// metadata can be any custom object
CompanyModel companyA = new CompanyBuilder()
    .companyId("67890")
    .companyDomain("acmeinc.com") // If set, Moesif will enrich your profiles with publicly available info 
    .metadata(APIHelper.deserialize("{" +
        "\"org_name\": \"Acme, Inc\"," +
        "\"plan_name\": \"Free\"," +
        "\"deal_stage\": \"Lead\"," +
        "\"mrr\": 24000," +
        "\"demographics\": {" +
            "\"alexa_ranking\": 500000," +
            "\"employee_count\": 47" +
          "}" +
        "}"))
    .build();

// Only companyId is required
// metadata can be any custom object
CompanyModel companyB = new CompanyBuilder()
    .companyId("09876")
    .companyDomain("contoso.com") // If set, Moesif will enrich your profiles with publicly available info 
    .metadata(APIHelper.deserialize("{" +
        "\"org_name\": \"Contoso, Inc\"," +
        "\"plan_name\": \"Free\"," +
        "\"deal_stage\": \"Lead\"," +
        "\"mrr\": 48000," +
        "\"demographics\": {" +
            "\"alexa_ranking\": 500000," +
            "\"employee_count\": 53" +
          "}" +
        "}"))
    .build();

  companies.add(companyA);
  companies.add(companyB);

  filter.updateCompaniesBatch(companies);
```

## Other integrations

To view more more documentation on integration options, please visit __[the Integration Options Documentation](https://www.moesif.com/docs/getting-started/integration-options/).__

[ico-built-for]: https://img.shields.io/badge/play-play%20framework-green
[ico-version]: https://img.shields.io/maven-central/v/com.moesif.filter/moesif-play-filter
[ico-license]: https://img.shields.io/badge/License-Apache%202.0-green.svg
[ico-source]: https://img.shields.io/github/last-commit/moesif/moesif-servlet.svg?style=social

[link-built-for]: https://www.playframework.com/
[link-package]: https://search.maven.org/artifact/com.moesif.filter/moesif-play-filter
[link-license]: https://raw.githubusercontent.com/Moesif/moesif-play-filter/master/LICENSE
[link-source]: https://github.com/moesif/moesif-play-filter
