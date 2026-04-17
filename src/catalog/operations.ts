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
