/**
 * Hand-written GraphQL operation documents. Field selection is intentionally
 * curated to stay LLM-context-friendly rather than mirroring every scalar on
 * the source type. Sub-selections are shallow by default; add more fields
 * only when a composite workflow genuinely needs them.
 *
 * TABLE fields:
 * - Summary set (used by list queries): identity, type, description, freshness,
 *   popularity signals, deprecation/verification state.
 * - Detail set (used by get-one queries): summary + descriptive text sources,
 *   ownership, tags, external links, schema/database context.
 */

const TABLE_SUMMARY_FIELDS = /* GraphQL */ `
  id
  name
  externalId
  schemaId
  description
  tableType
  url
  popularity
  lastRefreshedAt
  lastQueriedAt
  deletedAt
  deprecatedAt
  isDeprecated
  isVerified
`;

const TABLE_DETAIL_FIELDS = /* GraphQL */ `
  ${TABLE_SUMMARY_FIELDS}
  createdAt
  updatedAt
  descriptionRaw
  externalDescription
  externalDescriptionSource
  isDescriptionGenerated
  numberOfQueries
  tableSize
  slug
  verifiedAt
  verifiedByUserId
  deprecatedByUserId
  lastDescribedByUserId
  schema {
    id
    name
    databaseId
  }
  ownerEntities {
    id
    userId
    user {
      id
      email
      fullName
    }
  }
  teamOwnerEntities {
    id
    teamId
    team {
      id
      name
      email
    }
  }
  tagEntities {
    id
    tag {
      id
      label
      color
    }
  }
  externalLinks {
    id
    url
    technology
  }
  transformationSource
`;

export const UPDATE_TABLES = /* GraphQL */ `
  mutation CatalogUpdateTables($data: [UpdateTableInput!]!) {
    updateTables(data: $data) {
      id
      name
      externalDescription
      tableType
      url
    }
  }
`;

export const UPDATE_COLUMNS_METADATA = /* GraphQL */ `
  mutation CatalogUpdateColumnsMetadata($data: [UpdateColumnsMetadataInput!]!) {
    updateColumnsMetadata(data: $data) {
      id
      name
      description
      descriptionRaw
      externalDescription
      isPii
      isPrimaryKey
    }
  }
`;

export const GET_TABLES_SUMMARY = /* GraphQL */ `
  query CatalogGetTablesSummary(
    $scope: GetTablesScope
    $sorting: [TableSorting!]
    $pagination: Pagination
  ) {
    getTables(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        ${TABLE_SUMMARY_FIELDS}
      }
    }
  }
`;

export const GET_TABLE_DETAIL = /* GraphQL */ `
  query CatalogGetTableDetail($ids: [String!]!) {
    getTables(scope: { ids: $ids }, pagination: { nbPerPage: 1, page: 0 }) {
      data {
        ${TABLE_DETAIL_FIELDS}
      }
    }
  }
`;

// ── AI: semantic query search + assistant ───────────────────────────────

export const SEARCH_QUERIES = /* GraphQL */ `
  query CatalogSearchQueries(
    $data: SearchQueriesInput!
    $scope: SearchQueriesScope
  ) {
    searchQueries(data: $data, scope: $scope) {
      data {
        query
        tableIds
        author {
          name
          email
        }
      }
    }
  }
`;

export const ADD_AI_ASSISTANT_JOB = /* GraphQL */ `
  query CatalogAddAiAssistantJob($data: AddAiAssistantJobInput!) {
    addAiAssistantJob(data: $data) {
      data {
        id
        jobId
      }
    }
  }
`;

export const GET_AI_ASSISTANT_JOB_RESULT = /* GraphQL */ `
  query CatalogGetAiAssistantJobResult($data: JobResultInput!) {
    getAiAssistantJobResult(data: $data) {
      data {
        status
        answer
        assets {
          id
          name
          url
          internalLink
        }
      }
    }
  }
`;

// ── Governance: users, teams, quality checks, pinned assets ─────────────

export const CREATE_EXTERNAL_LINKS = /* GraphQL */ `
  mutation CatalogCreateExternalLinks($data: [CreateExternalLinkInput!]!) {
    createExternalLinks(data: $data) {
      id
      tableId
      technology
      url
    }
  }
`;

export const UPDATE_EXTERNAL_LINKS = /* GraphQL */ `
  mutation CatalogUpdateExternalLinks($data: [UpdateExternalLinkInput!]!) {
    updateExternalLinks(data: $data) {
      id
      tableId
      technology
      url
    }
  }
`;

export const DELETE_EXTERNAL_LINKS = /* GraphQL */ `
  mutation CatalogDeleteExternalLinks($data: [DeleteExternalLinkInput!]!) {
    deleteExternalLinks(data: $data)
  }
`;

export const UPSERT_DATA_QUALITIES = /* GraphQL */ `
  mutation CatalogUpsertDataQualities($data: UpsertQualityChecksInput!) {
    upsertDataQualities(data: $data) {
      id
      name
      description
      externalId
      tableId
      columnId
      status
      url
      runAt
    }
  }
`;

export const REMOVE_DATA_QUALITIES = /* GraphQL */ `
  mutation CatalogRemoveDataQualities($data: RemoveQualityChecksInput!) {
    removeDataQualities(data: $data)
  }
`;

export const GET_USERS = /* GraphQL */ `
  query CatalogGetUsers($pagination: Pagination) {
    getUsers(pagination: $pagination) {
      id
      firstName
      lastName
      email
      role
      status
      isEmailValidated
      createdAt
      ownedAssetIds
    }
  }
`;

export const GET_TEAMS = /* GraphQL */ `
  query CatalogGetTeams($pagination: Pagination) {
    getTeams(pagination: $pagination) {
      id
      name
      description
      email
      slackChannel
      slackGroup
      memberIds
      ownedAssetIds
      createdAt
    }
  }
`;

export const GET_DATA_QUALITIES = /* GraphQL */ `
  query CatalogGetDataQualities(
    $scope: GetQualityChecksScope
    $pagination: Pagination
  ) {
    getDataQualities(scope: $scope, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
        description
        externalId
        tableId
        columnId
        status
        result
        source
        ownerEmail
        url
        runAt
        createdAt
      }
    }
  }
`;

export const GET_PINNED_ASSETS = /* GraphQL */ `
  query CatalogGetPinnedAssets(
    $scope: GetEntitiesLinksScope
    $sorting: [EntitiesLinkSorting!]
    $pagination: Pagination
  ) {
    getPinnedAssets(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        fromTableId
        fromDashboardId
        fromTermId
        toTableId
        toDashboardId
        toDashboardFieldId
        toColumnId
        toTermId
        createdAt
        updatedAt
      }
    }
  }
`;

// ── Tags, terms, data products ───────────────────────────────────────────

export const ATTACH_TAGS = /* GraphQL */ `
  mutation CatalogAttachTags($data: [BaseTagEntityInput!]!) {
    attachTags(data: $data)
  }
`;

export const DETACH_TAGS = /* GraphQL */ `
  mutation CatalogDetachTags($data: [BaseTagEntityInput!]!) {
    detachTags(data: $data)
  }
`;

export const GET_TAGS = /* GraphQL */ `
  query CatalogGetTags(
    $scope: GetTagsScope
    $sorting: [TagSorting!]
    $pagination: Pagination
  ) {
    getTags(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        label
        color
        slug
        linkedTermId
        createdAt
        updatedAt
      }
    }
  }
`;

export const GET_TERMS = /* GraphQL */ `
  query CatalogGetTerms(
    $scope: GetTermsScope
    $sorting: [TermSorting!]
    $pagination: Pagination
  ) {
    getTerms(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
        description
        externalId
        icon
        parentTermId
        depthLevel
        isVerified
        isDeprecated
        isDescriptionGenerated
        slug
        createdAt
        updatedAt
        lastEditedAt
        deletedAt
        deprecatedAt
        linkedTag {
          id
          label
          color
        }
      }
    }
  }
`;

export const GET_DATA_PRODUCTS = /* GraphQL */ `
  query CatalogGetDataProducts(
    $scope: GetDataProductScope
    $sorting: [DataProductSorting!]
    $pagination: Pagination
  ) {
    getDataProducts(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        coverUrl
        tableId
        dashboardId
        termId
      }
    }
  }
`;

// ── Discovery: sources, databases, schemas ───────────────────────────────
// These three are intentionally flat (no sorting per SDL, no sub-relations)
// because they're used primarily to resolve IDs for other queries' scope
// filters. Keep selections compact.

export const GET_SOURCES = /* GraphQL */ `
  query CatalogGetSources($scope: GetSourcesScope, $pagination: Pagination) {
    getSources(scope: $scope, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
        slug
        type
        technology
        origin
        lastRefreshedAt
        deletedAt
        createdAt
      }
    }
  }
`;

export const GET_DATABASES = /* GraphQL */ `
  query CatalogGetDatabases($scope: GetDatabasesScope, $pagination: Pagination) {
    getDatabases(scope: $scope, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
        externalId
        slug
        sourceId
        description
        isHidden
        deletedAt
        createdAt
      }
    }
  }
`;

export const GET_SCHEMAS = /* GraphQL */ `
  query CatalogGetSchemas($scope: GetSchemasScope, $pagination: Pagination) {
    getSchemas(scope: $scope, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
        externalId
        databaseId
        description
        isHidden
        deletedAt
        createdAt
      }
    }
  }
`;

const DASHBOARD_SUMMARY_FIELDS = /* GraphQL */ `
  id
  name
  externalId
  externalSlug
  description
  type
  url
  folderPath
  sourceId
  popularity
  isVerified
  isDeprecated
  deletedAt
  deprecatedAt
`;

const DASHBOARD_DETAIL_FIELDS = /* GraphQL */ `
  ${DASHBOARD_SUMMARY_FIELDS}
  createdAt
  updatedAt
  descriptionRaw
  externalDescription
  externalDescriptionSource
  isDescriptionGenerated
  folderUrl
  slug
  verifiedAt
  verifiedByUserId
  deprecatedByUserId
  lastDescribedByUserId
  ownerEntities {
    id
    userId
    user {
      id
      email
      fullName
    }
  }
  teamOwnerEntities {
    id
    teamId
    team {
      id
      name
      email
    }
  }
  tagEntities {
    id
    tag {
      id
      label
      color
    }
  }
`;

export const GET_DASHBOARDS_SUMMARY = /* GraphQL */ `
  query CatalogGetDashboardsSummary(
    $scope: GetDashboardsScope
    $sorting: [DashboardSorting!]
    $pagination: Pagination
  ) {
    getDashboards(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        ${DASHBOARD_SUMMARY_FIELDS}
      }
    }
  }
`;

export const GET_DASHBOARD_DETAIL = /* GraphQL */ `
  query CatalogGetDashboardDetail($ids: [String!]!) {
    getDashboards(scope: { ids: $ids }, pagination: { nbPerPage: 1, page: 0 }) {
      data {
        ${DASHBOARD_DETAIL_FIELDS}
      }
    }
  }
`;

const COLUMN_SUMMARY_FIELDS = /* GraphQL */ `
  id
  name
  externalId
  tableId
  dataType
  description
  isNullable
  isPii
  isPrimaryKey
  isDescriptionGenerated
  sourceOrder
  deletedAt
`;

const COLUMN_DETAIL_FIELDS = /* GraphQL */ `
  ${COLUMN_SUMMARY_FIELDS}
  descriptionRaw
  externalDescription
  externalDescriptionSource
  sourceId
  describedByColumnId
  createdAt
  updatedAt
  tagEntities {
    id
    tag {
      id
      label
      color
    }
  }
`;

export const GET_COLUMNS_SUMMARY = /* GraphQL */ `
  query CatalogGetColumnsSummary(
    $scope: GetColumnsScope
    $sorting: [ColumnSorting!]
    $pagination: Pagination
  ) {
    getColumns(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        ${COLUMN_SUMMARY_FIELDS}
      }
    }
  }
`;

export const GET_COLUMN_DETAIL = /* GraphQL */ `
  query CatalogGetColumnDetail($ids: [String!]!) {
    getColumns(scope: { ids: $ids }, pagination: { nbPerPage: 1, page: 0 }) {
      data {
        ${COLUMN_DETAIL_FIELDS}
      }
    }
  }
`;

export const GET_COLUMN_JOINS = /* GraphQL */ `
  query CatalogGetColumnJoins(
    $scope: GetColumnJoinsScope
    $sorting: [ColumnJoinSorting!]
    $pagination: Pagination
  ) {
    getColumnJoins(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        count
        firstColumnId
        secondColumnId
        firstColumn {
          id
          name
          tableId
        }
        secondColumn {
          id
          name
          tableId
        }
      }
    }
  }
`;

export const UPSERT_LINEAGES = /* GraphQL */ `
  mutation CatalogUpsertLineages($data: [UpsertLineageInput!]!) {
    upsertLineages(data: $data) {
      id
      lineageType
      parentTableId
      parentDashboardId
      childTableId
      childDashboardId
      createdAt
      refreshedAt
    }
  }
`;

export const DELETE_LINEAGES = /* GraphQL */ `
  mutation CatalogDeleteLineages($data: [DeleteLineageInput!]!) {
    deleteLineages(data: $data)
  }
`;

export const GET_LINEAGES = /* GraphQL */ `
  query CatalogGetLineages(
    $scope: GetLineagesScope
    $sorting: [LineageSorting!]
    $pagination: Pagination
  ) {
    getLineages(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        lineageType
        parentTableId
        parentDashboardId
        childTableId
        childDashboardId
        createdAt
        refreshedAt
      }
    }
  }
`;

export const GET_FIELD_LINEAGES = /* GraphQL */ `
  query CatalogGetFieldLineages(
    $scope: GetFieldLineagesScope!
    $sorting: [FieldLineageSorting!]
    $pagination: Pagination
  ) {
    getFieldLineages(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        lineageType
        parentColumnId
        parentDashboardFieldId
        childColumnId
        childDashboardId
        childDashboardFieldId
        createdAt
        refreshedAt
      }
    }
  }
`;

export const GET_TABLE_QUERIES = /* GraphQL */ `
  query CatalogGetTableQueries(
    $scope: GetTableQueriesScope
    $sorting: [QuerySorting!]
    $pagination: Pagination
  ) {
    getTableQueries(scope: $scope, sorting: $sorting, pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        hash
        query
        queryType
        author
        sourceUserId
        queriesDuration
        rowsProduced
        snowflakeWarehouseSize
        databaseIds
        schemaIds
        isHidden
        tables {
          id
          name
          path
        }
      }
    }
  }
`;
