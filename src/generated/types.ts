export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type MakeEmpty<T extends { [key: string]: unknown }, K extends keyof T> = { [_ in K]?: never };
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
  /** The javascript `Date` as string. Type represents date and time as the ISO Date string. */
  DateTime: { input: string; output: string; }
  /** The `JSON` scalar type represents JSON values as specified by [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf). */
  JSON: { input: unknown; output: unknown; }
  /** The javascript `Date` as integer. Type represents date and time as number of milliseconds from start of UNIX epoch. */
  Timestamp: { input: any; output: any; }
};

/** Input object expected by the query or mutation */
export type AddAiAssistantJobInput = {
  /** The user's email. A Catalog account is required to interact with the AI Assistant, if the user doesn't have one, an error will be raised */
  email: Scalars['String']['input'];
  /** The external identifier acts as a unique key that links a conversation across multiple messages.       This enables the AI Assistant to maintain continuity and reuse context, allowing it to deliver       consistent and relevant responses throughout the conversation. */
  externalConversationId: Scalars['String']['input'];
  /** Origin of the job (only API or DUST allowed) */
  origin?: InputMaybe<Origin>;
  /** Question that will be used to find related assets. Must be less than 10000 characters */
  question: Scalars['String']['input'];
};

/** Response from the executed query or mutation */
export type AddAiAssistantJobOutput = {
  /** Field containing the response from the executed query or mutation */
  data: ConverseWithAssistantOutput;
};

/** Status and result of the AI Assistant job */
export type AiAssistantJobResult = {
  /** The answer to the question */
  answer: Scalars['String']['output'];
  /** The list of assets referenced to generate the answer */
  assets: Array<AssetWithInternalLink>;
  /** The current status of the job */
  status: JobStatus;
};

/** Assets referenced in the AI Assistant job result */
export type AssetWithInternalLink = {
  /** ID of the asset */
  id: Scalars['String']['output'];
  /** The internal link of the asset (link to the asset in Catalog) */
  internalLink: Scalars['String']['output'];
  /** The name of the asset */
  name: Scalars['String']['output'];
  /** The url of the asset */
  url: Scalars['String']['output'];
};

/** Input object expected by the query or mutation */
export type BaseQualityCheckInput = {
  /** Column id linked to the quality check. */
  columnId?: InputMaybe<Scalars['String']['input']>;
  /** Quality check description */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The technical quality check identifier */
  externalId: Scalars['String']['input'];
  /** Quality check name */
  name: Scalars['String']['input'];
  /** Time at which the quality check ran */
  runAt: Scalars['DateTime']['input'];
  /** Status of the quality check */
  status: QualityStatus;
  /** Url of the quality check */
  url?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type BaseTagEntityInput = {
  /** The id of the tagged entity */
  entityId: Scalars['String']['input'];
  /** The type of the tagged entity */
  entityType: TagEntityType;
  /** The label of the tag */
  label: Scalars['String']['input'];
};

/** Available colors for tag */
export type Colors =
  | 'ALERT_COLOR'
  | 'AQUA_BLUE'
  | 'BLUE_RED'
  | 'CERULEAN_BLUE'
  | 'CHARTREUSE'
  | 'MAGENTA'
  | 'ORANGE_RED'
  | 'ORANGE_YELLOW'
  | 'RED_VIOLET'
  | 'SPRING_GREEN'
  | 'VIOLET';

/** A column represents a single field within a table. Each column is linked to a table and contains metadata such as data type, description, and technical identifiers. */
export type Column = {
  /** The column belongs to this account ID */
  accountId: Scalars['String']['output'];
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The data type for the column. For example: INTEGER */
  dataType: Scalars['String']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The column is described by this column id */
  describedByColumnId?: Maybe<Scalars['String']['output']>;
  /** The column custom description or the external one if empty */
  description: Scalars['String']['output'];
  /** The Catalog documentation for this column */
  descriptionRaw?: Maybe<Scalars['String']['output']>;
  /** The documentation from the source for this column */
  externalDescription?: Maybe<Scalars['String']['output']>;
  /** The source of the documentation for this column */
  externalDescriptionSource?: Maybe<ColumnExternalDescriptionSource>;
  /** The technical column identifier from the warehouse internal structure */
  externalId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** True if the description has been generated, false otherwise */
  isDescriptionGenerated?: Maybe<Scalars['Boolean']['output']>;
  /** True if this column is nullable */
  isNullable: Scalars['Boolean']['output'];
  /** True if this column is PII related */
  isPii?: Maybe<Scalars['Boolean']['output']>;
  /** True if this column is a primary key */
  isPrimaryKey?: Maybe<Scalars['Boolean']['output']>;
  /** The column name */
  name: Scalars['String']['output'];
  /** The column belongs to this source ID */
  sourceId: Scalars['String']['output'];
  /** The original column position index as imported from the source warehouse */
  sourceOrder?: Maybe<Scalars['Int']['output']>;
  /** The column belongs to this table ID */
  table: Table;
  /** The column belongs to this table ID */
  tableId: Scalars['String']['output'];
  /** The tag entities belonging to this column */
  tagEntities: Array<TagEntity>;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** Column External Sources of documentation available */
export type ColumnExternalDescriptionSource =
  /** Documentation from Catalog public API */
  | 'API'
  /** Documentation generated by Catalog */
  | 'CASTOR'
  /** Documentation from your Coalesce */
  | 'COALESCE'
  /** Documentation from your warehouse */
  | 'DATABASE'
  /** Documentation from your dbt */
  | 'DBT'
  /** Documentation from your Looker */
  | 'LOOKER';

/** Represents a join relationship between two columns, including the number of times the join occurs. Each ColumnJoin links two columns and provides metadata about their association. */
export type ColumnJoin = {
  /** The number of join linking the columns */
  count: Scalars['Int']['output'];
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The first column on which the join happens */
  firstColumn: Column;
  /** The first column id on which the join happens */
  firstColumnId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The second column on which the join happens */
  secondColumn: Column;
  /** The second column id on which the join happens */
  secondColumnId: Scalars['String']['output'];
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** Sorting options for results */
export type ColumnJoinSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: ColumnJoinSortingKey;
};

/** The possible sort fields */
export type ColumnJoinSortingKey =
  /** Sort by the number of join occurrences */
  | 'count';

/** Sorting options for results */
export type ColumnSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: ColumnSortingKey;
};

/** The possible sort fields */
export type ColumnSortingKey =
  | 'name'
  | 'sourceOrder'
  | 'tableName'
  | 'tablePopularity';

/** Information about the started AI Assistant job */
export type ConverseWithAssistantOutput = {
  /** The assistant message ID (answer) */
  id: Scalars['String']['output'];
  /** ID of the job processing the response. Can be used to pull the job result */
  jobId: Scalars['String']['output'];
};

/** Input object expected by the query or mutation */
export type CreateColumnInput = {
  /** The data type for the column. For example: INTEGER */
  dataType: Scalars['String']['input'];
  /** If present, indicates when the column was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The external documentation for this column */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** The technical column identifier from the warehouse internal structure */
  externalId: Scalars['String']['input'];
  /** True if this column is nullable */
  isNullable: Scalars['Boolean']['input'];
  /** The PII status to apply to the column */
  isPii?: InputMaybe<Scalars['Boolean']['input']>;
  /** The PrimaryKey status to apply to the column */
  isPrimaryKey?: InputMaybe<Scalars['Boolean']['input']>;
  /** The column name */
  name: Scalars['String']['input'];
  /** The original column position index as imported from the source warehouse */
  sourceOrder?: InputMaybe<Scalars['Int']['input']>;
  /** The column belongs to this table ID */
  tableId: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type CreateDatabaseInput = {
  /** If present, indicates when the database was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The Catalog documentation for this database (max length: 500) */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The technical database identifier from the warehouse structure */
  externalId: Scalars['String']['input'];
  /** Indicate if the database should be hidden */
  isHidden?: InputMaybe<Scalars['Boolean']['input']>;
  /** The database name */
  name: Scalars['String']['input'];
  /** The id of the parent warehouse */
  sourceId: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type CreateExternalLinkInput = {
  /** The table to which the link refers */
  tableId: Scalars['String']['input'];
  /** The origin of the link */
  technology: ExternalLinkTechnology;
  /** the url of the link */
  url: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type CreateSchemaInput = {
  /** The id of the parent database */
  databaseId: Scalars['String']['input'];
  /** If present, indicates when the schema was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The Catalog documentation for this schema */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The technical schema identifier from the warehouse schema structure */
  externalId: Scalars['String']['input'];
  /** Whether the schema should be hidden */
  isHidden?: InputMaybe<Scalars['Boolean']['input']>;
  /** The schema name */
  name: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type CreateSourceInput = {
  /** The name of the source to add */
  name: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type CreateTableInput = {
  /** The documentation from the source for this table */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** The technical table identifier from the warehouse table structure */
  externalId: Scalars['String']['input'];
  /** The last known datetime of refresh for this table */
  lastRefreshedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The table name */
  name: Scalars['String']['input'];
  /** The number of queries on which popularity is based */
  numberOfQueries?: InputMaybe<Scalars['Int']['input']>;
  /** The table popularity rated out of 1000000 */
  popularity?: InputMaybe<Scalars['Int']['input']>;
  /** The schema ID to link with the table that belongs to. Required at table creation. Cannot be modified later */
  schemaId: Scalars['String']['input'];
  /** The size of the table (in megabytes) */
  tableSize?: InputMaybe<Scalars['Int']['input']>;
  /** The type of this table */
  tableType?: InputMaybe<TableType>;
  /** The external url to the table */
  url?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type CreateTermInput = {
  /** Description for the term, supports markdown */
  description: Scalars['String']['input'];
  /** ID of the tag to link to the term */
  linkedTagId?: InputMaybe<Scalars['String']['input']>;
  /** Name of the term */
  name: Scalars['String']['input'];
  /** ID of the parent term, no value will create a root term */
  parentTermId?: InputMaybe<Scalars['String']['input']>;
};

/** A dashboard represents a collection of visualizations and reports, providing insights into data. Each dashboard contains metadata, documentation, and links to its source and related entities. */
export type Dashboard = {
  createdAt: Scalars['Timestamp']['output'];
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The time at which the dashboard was deprecated */
  deprecatedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The dashboard was deprecated by the user belonging to this ID */
  deprecatedByUserId?: Maybe<Scalars['String']['output']>;
  /** The dashboard custom rawDescription or the external one if empty */
  description: Scalars['String']['output'];
  /** The Catalog documentation for this dashboard, as raw text. Can contains Markdown. */
  descriptionRaw?: Maybe<Scalars['String']['output']>;
  /** Catalog description in Lexical format (see https://lexical.dev/ documentation) */
  descriptionStateLexical?: Maybe<Scalars['JSON']['output']>;
  entityEditors: Array<EntityEditor>;
  /** The documentation from the source for this dashboard */
  externalDescription?: Maybe<Scalars['String']['output']>;
  /** The source of the documentation for this dashboard */
  externalDescriptionSource?: Maybe<DashboardExternalDescriptionSource>;
  /** The external ID of the dashboard */
  externalId: Scalars['String']['output'];
  /** The external slug of the dashboard */
  externalSlug?: Maybe<Scalars['String']['output']>;
  /** The folder path inside the external data source. Format is root/folder1/folder2 */
  folderPath: Scalars['String']['output'];
  /** The url of the folder where the dashboard lies in the source */
  folderUrl?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Whether this dashboard has been marked as deprecated */
  isDeprecated: Scalars['Boolean']['output'];
  /** True if the description has been generated, false otherwise */
  isDescriptionGenerated?: Maybe<Scalars['Boolean']['output']>;
  /** Whether this dashboard has been marked as verified */
  isVerified: Scalars['Boolean']['output'];
  /** The dashboard was last described be the user belonging to this ID */
  lastDescribedByUserId?: Maybe<Scalars['String']['output']>;
  /** The dashboard name */
  name: Scalars['String']['output'];
  /** The individual owners of the dashboard */
  ownerEntities: Array<OwnerEntity>;
  /** ·The dashboard popularity rated out of 1000000 */
  popularity?: Maybe<Scalars['Int']['output']>;
  /** Unique Catalog slug of the resource */
  slug: Scalars['String']['output'];
  /** The dashboard belongs to this data source */
  source: Source;
  /** The dashboard belongs to this data source ID */
  sourceId: Scalars['String']['output'];
  /** The tag entities belonging to this dashboard */
  tagEntities: Array<TagEntity>;
  /** The team owners of the dashboard */
  teamOwnerEntities: Array<TeamOwnerEntity>;
  /** The dashboard's type */
  type: DashboardType;
  updatedAt: Scalars['Timestamp']['output'];
  /** The dashboard url in the source */
  url: Scalars['String']['output'];
  /** The time at which the dashboard was verified */
  verifiedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The dashboard is verified by this user */
  verifiedBy?: Maybe<User>;
  /** The dashboard is verified by the user belonging to this ID */
  verifiedByUserId?: Maybe<Scalars['String']['output']>;
};

/** Dashboard External Sources of documentation available */
export type DashboardExternalDescriptionSource =
  /** Generated by Catalog */
  | 'CASTOR'
  /** From your visualization tool */
  | 'VISUALIZATION';

/** A dashboard field represents a single field within a dashboard. Each dashboard field is linked to a dashboard and contains metadata such as data type, description, and technical identifier. */
export type DashboardField = {
  createdAt: Scalars['Timestamp']['output'];
  /** The field belongs to this dashboard */
  dashboard: Dashboard;
  dashboardId: Scalars['String']['output'];
  /** The data type for the dashboard field */
  dataType: Scalars['String']['output'];
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The Catalog documentation for this column */
  description?: Maybe<Scalars['String']['output']>;
  /** The technical identifier within the source */
  externalId: Scalars['String']['output'];
  id: Scalars['ID']['output'];
  /** True if this field is a primary key */
  isPrimaryKey?: Maybe<Scalars['Boolean']['output']>;
  /** The dashboard field label */
  label: Scalars['String']['output'];
  /** The dashboard field name */
  name: Scalars['String']['output'];
  /** Field popularity */
  popularity?: Maybe<Scalars['Int']['output']>;
  slug: Scalars['String']['output'];
  updatedAt: Scalars['Timestamp']['output'];
  /** Name of the view used for display */
  viewLabel?: Maybe<Scalars['String']['output']>;
  /** Technical name of the view */
  viewName?: Maybe<Scalars['String']['output']>;
};

/** Sorting options for results */
export type DashboardSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: DashboardSortingKey;
};

/** The possible sort fields */
export type DashboardSortingKey =
  /** Sort by the dashboard name */
  | 'name'
  /** Sort by the number of owners and team owners */
  | 'ownersAndTeamOwnersCount'
  /** Sort by the dashboard popularity */
  | 'popularity';

/** The flavour of dashboard (e.g. tableau) */
export type DashboardType =
  /** Visualization composed of one or more tiles */
  | 'DASHBOARD'
  /** Atomic visualization component */
  | 'TILE'
  /** Visualization modeling layer with fields */
  | 'VIZ_MODEL';

export type DataProduct = {
  /** The data product’s cover URL */
  coverUrl?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The dashboard marked as data product */
  dashboard?: Maybe<Dashboard>;
  /** The dashboard id marked as data product */
  dashboardId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The table marked as data product */
  table?: Maybe<Table>;
  /** The table id marked as data product */
  tableId?: Maybe<Scalars['String']['output']>;
  /** The knowledge page marked as data product */
  term?: Maybe<Term>;
  /** The knowledge page id marked as data product */
  termId?: Maybe<Scalars['String']['output']>;
};

/** Sorting options for results */
export type DataProductSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: DataProductSortingKey;
};

/** The possible sort fields for Data Products */
export type DataProductSortingKey =
  /** Sort by the dashboard name */
  | 'dashboardName'
  /** Sort by the table name */
  | 'tableName'
  /** Sort by the term name */
  | 'termName';

/** A database represents a collection of tables and views, providing insights into data. Each database contains metadata, documentation, and links to its source and related entities. */
export type Database = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The Catalog documentation for this database */
  description?: Maybe<Scalars['String']['output']>;
  /** The technical database identifier from the warehouse database structure */
  externalId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Whether this database is hidden */
  isHidden: Scalars['Boolean']['output'];
  /** The technical database identifier from the warehouse database structure */
  name: Scalars['String']['output'];
  /** Unique Catalog slug of the resource */
  slug: Scalars['String']['output'];
  /** The source (warehouse) ID of the database */
  sourceId: Scalars['String']['output'];
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
  /** The warehouse of the database */
  warehouse: Source;
};

/** Input object expected by the query or mutation */
export type DeleteExternalLinkInput = {
  /** The id of the link to delete */
  id: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type DeleteLineageInput = {
  /** The id of the children dashboard of the lineage */
  childDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the children table of the lineage */
  childTableId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the parent dashboard of the lineage */
  parentDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the parent table of the lineage */
  parentTableId?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type DeleteTermInput = {
  /** id of the term to delete */
  id: Scalars['String']['input'];
};

export type EntitiesLink = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The dashboard the link is issued from */
  fromDashboard?: Maybe<Dashboard>;
  /** The dashboard id the link is issued from */
  fromDashboardId?: Maybe<Scalars['String']['output']>;
  /** The table the link is issued from */
  fromTable?: Maybe<Table>;
  /** The table id the link is issued from */
  fromTableId?: Maybe<Scalars['String']['output']>;
  /** The knowledge page the link is issued from */
  fromTerm?: Maybe<Term>;
  /** The knowledge page id the link is issued from */
  fromTermId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The column the link points to */
  toColumn?: Maybe<Column>;
  /** The column id the link points to */
  toColumnId?: Maybe<Scalars['String']['output']>;
  /** The dashboard the link points to */
  toDashboard?: Maybe<Dashboard>;
  /** The dashboard field the link points to */
  toDashboardField?: Maybe<DashboardField>;
  /** The dashboard field id the link points to */
  toDashboardFieldId?: Maybe<Scalars['String']['output']>;
  /** The dashboard id the link points to */
  toDashboardId?: Maybe<Scalars['String']['output']>;
  /** The table the link points to */
  toTable?: Maybe<Table>;
  /** The table id the link points to */
  toTableId?: Maybe<Scalars['String']['output']>;
  /** The knowledge page the link points to */
  toTerm?: Maybe<Term>;
  /** The knowledge page id the link points to */
  toTermId?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** Input object expected by the query or mutation */
export type EntitiesLinkInput = {
  /** The entity from which the entitiesLink is issued */
  from: EntitiesLinkTargetInput;
  /** The entity to which the entitiesLink points */
  to: EntitiesLinkTargetInput;
};

/** Sorting options for results */
export type EntitiesLinkSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: EntitiesLinkSortingKey;
};

/** Entities links possible sort fields */
export type EntitiesLinkSortingKey =
  /** Sort by the entities link creation date */
  | 'createdAt';

/** Input object expected by the query or mutation */
export type EntitiesLinkTargetInput = {
  /** The target entity id */
  id: Scalars['String']['input'];
  /** The target entity type */
  type: EntitiesLinkTargetType;
};

/** The type of entity as the target for an EntityLink */
export type EntitiesLinkTargetType =
  /** Column entity */
  | 'COLUMN'
  /** Dashboard entity */
  | 'DASHBOARD'
  | 'DASHBOARD_FIELD'
  /** Table entity */
  | 'TABLE'
  /** Knowledge entity */
  | 'TERM';

/** Represents a user who is designated as an editor for a specific entity (dashboard, table, or knowledge). */
export type EntityEditor = {
  createdAt: Scalars['Timestamp']['output'];
  dashboardId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Associated sourceUser */
  sourceUser: SourceUser;
  /** ID of the user editor for this entity */
  sourceUserId: Scalars['String']['output'];
  tableId?: Maybe<Scalars['String']['output']>;
  termId?: Maybe<Scalars['String']['output']>;
  updatedAt: Scalars['Timestamp']['output'];
};

/** Input object expected by the query or mutation */
export type EntityTarget = {
  /** The id of the entity */
  entityId?: InputMaybe<Scalars['String']['input']>;
  /** The type of the entity */
  entityType?: InputMaybe<EntityTargetType>;
};

/** The scope of the action */
export type EntityTargetType =
  /** Dashboard entity */
  | 'DASHBOARD'
  /** Table entity */
  | 'TABLE'
  /** Knowledge entity */
  | 'TERM';

/** An external link for a table represents a URL to an external system such as github */
export type ExternalLink = {
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The link belongs to this data table ID */
  tableId: Scalars['String']['output'];
  /** The origin of the url */
  technology: ExternalLinkTechnology;
  /** The external link url */
  url: Scalars['String']['output'];
};

/** The possible external link technologies */
export type ExternalLinkTechnology =
  /** Apache Airflow workflow management platform */
  | 'AIRFLOW'
  /** GitHub version control and collaboration platform */
  | 'GITHUB'
  /** GitLab version control and DevOps platform */
  | 'GITLAB'
  /** Other external link type */
  | 'OTHER';

/** Field lineage link between your assets */
export type FieldLineage = {
  /** The id of the child column if it is a column lineage */
  childColumnId?: Maybe<Scalars['String']['output']>;
  /** The id of the child dashboard field if it is a dashboard field lineage */
  childDashboardFieldId?: Maybe<Scalars['String']['output']>;
  /** The id of the child dashboard if it is a dashboard lineage */
  childDashboardId?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt?: Maybe<Scalars['Timestamp']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The type of the lineage */
  lineageType?: Maybe<LineageType>;
  /** The id of the parent column if it is a column lineage */
  parentColumnId?: Maybe<Scalars['String']['output']>;
  /** The id of the parent dashboard field if it is a dashboard field lineage */
  parentDashboardFieldId?: Maybe<Scalars['String']['output']>;
  /** The last date on which this field lineage was recomputed */
  refreshedAt?: Maybe<Scalars['Timestamp']['output']>;
};

/** Sorting options for results */
export type FieldLineageSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: FieldLineageSortingKey;
};

/** The possible sort fields for field lineages */
export type FieldLineageSortingKey =
  /** Sort by the child dashboard popularity */
  | 'childDashboardPopularity'
  /** Sort by the field lineage id */
  | 'id'
  /** Sort by the field lineage popularity */
  | 'popularity'
  /** Sort by the field lineage type */
  | 'type';

/** How filter should capture provided table IDs */
export type FilterTablesMode =
  /** All provided table IDs must be used by retrieved queries */
  | 'ALL'
  /** At least one provided table IDs must be used by retrieved queries */
  | 'ANY';

/** Response from the executed query or mutation */
export type GetAiAssistantJobResultOutput = {
  /** Field containing the response from the executed query or mutation */
  data: AiAssistantJobResult;
};

/** Paginated response from the executed query or mutation */
export type GetColumnJoinsOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<ColumnJoin>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetColumnJoinsScope = {
  /** Filter upon first and second column ids */
  columnIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Filter upon column join ids */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Filter upon table ids */
  tableIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetColumnsOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Column>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetColumnsScope = {
  /** Scope by database */
  databaseId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by database IDs */
  databaseIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by description */
  description?: InputMaybe<Scalars['String']['input']>;
  /** Scope by hasColumnJoins */
  hasColumnJoins?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by ids */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by documentation status (either documented in the Catalog or an external source) */
  isDocumented?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by isPii (personally identifiable information) */
  isPii?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by isPrimaryKey */
  isPrimaryKey?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by column name */
  name?: InputMaybe<Scalars['String']['input']>;
  /** Scope by a substring of the column name */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by schema ID */
  schemaId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by schema IDs */
  schemaIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by source ID */
  sourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by source IDs */
  sourceIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by table ID */
  tableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by table IDs */
  tableIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetDashboardsOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Dashboard>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetDashboardsScope = {
  /** Scope by the dashboard folder path, start from the root folder */
  folderPath?: InputMaybe<Scalars['String']['input']>;
  /** Scope by multiple dashboard IDs */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by a substring of the dashboard name, case insensitive */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by source ID */
  sourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetDataProductOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<DataProduct>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetDataProductScope = {
  /** The type of the entities */
  entityType?: InputMaybe<EntityTargetType>;
  /** Scope by tag or domainid */
  withTagId?: InputMaybe<Scalars['String']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetDatabasesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Database>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetDatabasesScope = {
  /** Scope by a substring of the field name */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by a list of source ids */
  sourceIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetEntitiesLinkOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<EntitiesLink>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

export type GetEntitiesLinksScope = {
  /** Scope by dashboard the entities link is issued from */
  fromDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by table the entities link is issued from */
  fromTableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by term the entities link is issued from */
  fromTermId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by multiple term IDs from which the entity links are issued */
  fromTermIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by the parent table of columns pointed to by the entities */
  toColumnsOfTableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by dashboard field the entities link points to */
  toDashboardFieldId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by dashboard the entities link points to */
  toDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by the parent dashboard of fields pointed to by the entities */
  toFieldsOfDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by table the entities link points to */
  toTableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by term the entities link points to */
  toTermId?: InputMaybe<Scalars['String']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetFieldLineagesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<FieldLineage>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetFieldLineagesScope = {
  /** Scope by child column */
  childColumnId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by child dashboard field */
  childDashboardFieldId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by child dashboard field source */
  childDashboardFieldSourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by child dashboard source */
  childDashboardSourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by column source (PARENT / CHILD) */
  columnSourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope on lineages with a dashboard child */
  hasDashboardChild?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope on lineages type */
  lineageType?: InputMaybe<LineageType>;
  /** Scope by parent column */
  parentColumnId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by parent dashboard field */
  parentDashboardFieldId?: InputMaybe<Scalars['String']['input']>;
  /** Scope to filter on child asset type */
  withChildAssetType?: InputMaybe<LineageAssetType>;
};

/** Paginated response from the executed query or mutation */
export type GetLineagesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Lineage>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetLineagesScope = {
  /** Scope by child dashboard ID */
  childDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by child source ID */
  childSourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by child table ID */
  childTableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by multiple lineage IDs */
  lineageIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by lineage type */
  lineageType?: InputMaybe<LineageType>;
  /** Scope by parent dashboard ID */
  parentDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by parent source ID */
  parentSourceId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by parent table ID */
  parentTableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by the type of the child asset */
  withChildAssetType?: InputMaybe<LineageAssetType>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetQualityChecksOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<QualityCheck>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetQualityChecksScope = {
  /** Scope by table ID */
  tableId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetSchemasOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Schema>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetSchemasScope = {
  /** Scope by a list of database ids */
  databaseIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by a substring of the field name */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by a list of source ids */
  sourceIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetSourcesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Source>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetSourcesScope = {
  /** Scope by a substring of the source name */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by source origin */
  origin?: InputMaybe<SourceOrigin>;
  /** Scope by source technology */
  technology?: InputMaybe<SourceTechnology>;
  /** Scope by source type */
  type?: InputMaybe<SourceType>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetTableQueriesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<TableQueryOutput>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetTableQueriesScope = {
  /** Scope by database ID */
  databaseId?: InputMaybe<Scalars['String']['input']>;
  /** Filter by query type (SELECT or WRITE) */
  queryType?: InputMaybe<QueryType>;
  /** Scope by schema ID */
  schemaId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by specific table IDs (max 50 ids) */
  tableIds?: InputMaybe<Array<Scalars['String']['input']>>;
  /** how to filter by table IDs - default is ALL */
  tableIdsFilterMode?: InputMaybe<SearchArrayFilterMode>;
  /** Scope by warehouse ID */
  warehouseId?: InputMaybe<Scalars['String']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetTablesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Table>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetTablesScope = {
  /** Scope by database ID */
  databaseId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by multiple table IDs */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by a substring of the table name, case insensitive */
  nameContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by a substring of the table path, case insensitive */
  pathContains?: InputMaybe<Scalars['String']['input']>;
  /** Scope by schema ID */
  schemaId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by warehouse ID */
  warehouseId?: InputMaybe<Scalars['String']['input']>;
  /** Scope by withDeleted, whether to include deleted assets */
  withDeleted?: InputMaybe<Scalars['Boolean']['input']>;
  /** Scope by withHidden, whether to include hidden assets */
  withHidden?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Paginated response from the executed query or mutation */
export type GetTagsOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Tag>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetTagsScope = {
  /** Scope by multiple tag IDs */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by a substring of the tag label, case insensitive */
  labelContains?: InputMaybe<Scalars['String']['input']>;
};

/** Response from the executed query or mutation */
export type GetTeamsOutput = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The description of the team */
  description?: Maybe<Scalars['String']['output']>;
  /** The email of the team */
  email?: Maybe<Scalars['String']['output']>;
  /** the unique id of the team */
  id: Scalars['String']['output'];
  /** List of users ids that are members */
  memberIds: Array<Scalars['String']['output']>;
  /** The name of the team */
  name: Scalars['String']['output'];
  /** List of entities ids owned by the team */
  ownedAssetIds: Array<Scalars['String']['output']>;
  /** The slack channel of the team (start with #) */
  slackChannel?: Maybe<Scalars['String']['output']>;
  /** The slack group of the team (start with @) */
  slackGroup?: Maybe<Scalars['String']['output']>;
};

/** Paginated response from the executed query or mutation */
export type GetTermsOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<Term>;
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['output'];
  /** The page number (start at 0) */
  page?: Maybe<Scalars['Int']['output']>;
  /** The total number of entities */
  totalCount: Scalars['Int']['output'];
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type GetTermsScope = {
  /** Scope by multiple term IDs */
  ids?: InputMaybe<Array<Scalars['String']['input']>>;
  /** Scope by a substring of the term name, case insensitive */
  nameContains?: InputMaybe<Scalars['String']['input']>;
};

/** Response from the executed query or mutation */
export type GetUsersOutput = {
  /** The date and time the user was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The email of the user */
  email: Scalars['String']['output'];
  /** The first name of the user */
  firstName: Scalars['String']['output'];
  /** The ID of the user */
  id: Scalars['String']['output'];
  /** Whether the user has validated their email */
  isEmailValidated: Scalars['Boolean']['output'];
  /** The last name of the user */
  lastName: Scalars['String']['output'];
  /** List of entities owned by the user */
  ownedAssetIds: Array<Scalars['String']['output']>;
  /** The role of the user */
  role: UserRole;
  /** The status of the user */
  status: UserStatus;
  /** ids of the teams this user belongs to */
  teamIds: Array<Scalars['String']['output']>;
};

/** Input object expected by the query or mutation */
export type JobResultInput = {
  /** The number of seconds to delay before returning the result if the job is not finished */
  delaySeconds?: InputMaybe<Scalars['Int']['input']>;
  /** Job ID to retrieve the result for */
  id: Scalars['String']['input'];
};

/** The status of the worker job */
export type JobStatus =
  /** Job is currently being processed */
  | 'ACTIVE'
  /** Job has been added to the queue */
  | 'ADDED'
  /** Job has finished successfully */
  | 'COMPLETED'
  /** Job has failed during execution */
  | 'FAILED'
  /** Job has failed after all retry attempts */
  | 'RETRIES_EXHAUSTED';

/** Lineage link between your assets */
export type Lineage = {
  /** The id of the child dashboard if it is a dashboard lineage */
  childDashboardId?: Maybe<Scalars['String']['output']>;
  /** The id of the child table if it is a table lineage */
  childTableId?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt?: Maybe<Scalars['Timestamp']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The type of the lineage */
  lineageType?: Maybe<LineageType>;
  /** The id of the parent dashboard if it is a dashboard lineage */
  parentDashboardId?: Maybe<Scalars['String']['output']>;
  /** The id of the parent table if it is a table lineage */
  parentTableId?: Maybe<Scalars['String']['output']>;
  /** The last date on which this lineage was recomputed */
  refreshedAt?: Maybe<Scalars['Timestamp']['output']>;
};

/** The asset type for the lineage */
export type LineageAssetType =
  | 'COLUMN'
  | 'DASHBOARD'
  | 'DASHBOARD_FIELD'
  | 'TABLE';

/** Sorting options for results */
export type LineageSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: LineageSortingKey;
};

/** The possible sort fields for lineages */
export type LineageSortingKey =
  /** Sort by the lineage id */
  | 'id'
  /** Sort by the lineage popularity (based on the parent and child assets popularity) */
  | 'popularity'
  /** Sort by the lineage type */
  | 'type';

/** The type of the lineage */
export type LineageType =
  /** Detected automatically by Catalog */
  | 'AUTOMATIC'
  /** Created via the public API */
  | 'MANUAL_CUSTOMER'
  /** Created by the Catalog operations team */
  | 'MANUAL_OPS'
  /** Imported from other technologies */
  | 'OTHER_TECHNOS';

export type Mutation = {
  /** Add users to a Team */
  addTeamUsers: Scalars['Boolean']['output'];
  /** Attach tags to entities by using their tag labels, and create a new tag if it does not already exist. */
  attachTags: Scalars['Boolean']['output'];
  /**
   * Create many columns
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createColumns: Array<Column>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation will allow you to create batches of databases to administrate through this API.
   *
   * Note that in order not to conflict with Catalog’s extraction system, you will only be able to create databases on warehouses created from this API.
   *
   * You will also need to craft and provide custom external ids used as technical identifiers.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createDatabases: Array<Database>;
  /**
   * Create many External Links
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createExternalLinks: Array<ExternalLink>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation will allow you to create batches of schemas to administrate through this API.
   *
   * Note that in order not to conflict with Catalog’s extraction system, you will only be able to create schemas on databases and warehouses created through this API.
   *
   * You will also need to craft and provide custom external ids used as technical identifiers.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createSchemas: Array<Schema>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation allows you to create a single custom warehouse source to administrate through this API.
   *
   * This comes with some restrictions:
   *
   * - The created source will have a few fields with enforced values:
   *   - `origin` set to `API`
   *   - `type` set to `WAREHOUSE`
   *   - `technology` set to `GENERIC_WAREHOUSE`
   * - You will only be allowed **a single source** from this API (use the `updateSource` mutation to edit or delete it)
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createSource: Source;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation will allow you to create batches of tables to administrate through this API.
   *
   * Note that in order not to conflict with Catalog’s extraction system, you will only be able to create tables on schemas, databases and warehouses created through this API.
   *
   * You will also need to craft and provide custom external ids used as technical identifiers.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createTables: Array<Table>;
  /**
   * Create a Term either at root level (`parentTermId` is `null`) or as children of a given Term.
   *
   * - `name` is mandatory
   * - `description` is mandatory and supports markdown
   * - `linkedTagId` will override any linked term on the given tag
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  createTerm: Term;
  /**
   * Delete many External Links
   *
   *
   */
  deleteExternalLinks: Scalars['Boolean']['output'];
  /**
   * Delete many Lineages
   *
   *
   */
  deleteLineages: Scalars['Boolean']['output'];
  /** Delete an existing Term, `id` of a valid Term is necessary. This operation cannot be reversed. */
  deleteTerm: Scalars['Boolean']['output'];
  /** Detach tags from entities by using tag labels, and delete the tag if there are no entities attached to it anymore. */
  detachTags: Scalars['Boolean']['output'];
  /**
   * Delete many Quality Checks
   *
   *
   */
  removeDataQualities: Scalars['Boolean']['output'];
  /**
   * Delete many pinned assets
   *
   *
   */
  removePinnedAssets: Scalars['Boolean']['output'];
  /** Remove team ownership to assets in Catalog */
  removeTeamOwners: Scalars['Boolean']['output'];
  /** Remove users from a Team */
  removeTeamUsers: Scalars['Boolean']['output'];
  /**
   * Delete many User Owners
   *
   *
   */
  removeUserOwners: Scalars['Boolean']['output'];
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * Update many column external descriptions
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `tagEntities`
   * - `tagEntities.tag`
   * @deprecated Use updateColumnsMetadata instead
   */
  updateColumnDescriptions: Array<Column>;
  /**
   * Update many columns
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateColumns: Array<Column>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * Update many column metadata (description, external description, PII, primary key)
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `tagEntities`
   * - `tagEntities.tag`
   */
  updateColumnsMetadata: Array<Column>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation will allow you to batch update the databases you created.
   *
   * Note that in order not to mess with data ingested by Catalog, only the databases belonging to sources created from the API will be editable.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateDatabases: Array<Database>;
  /**
   * Update many External Links
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateExternalLinks: Array<ExternalLink>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation will allow you to batch update the schemas you created.
   *
   * Note that in order not to mess with data ingested by Catalog, only the schemas belonging to sources and databases created from this API will be editable.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateSchemas: Array<Schema>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * This mutation allows you to update the API source you created.
   * Note that in order not to mess with data ingested by Catalog, only the source created from the API will be editable.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateSource: Source;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * Update many tables external description
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateTableDescriptions: Array<Table>;
  /**
   * :::warning[Restricted Access]
   *
   * This endpoint access is restricted. Please reach out to your point of contact at Catalog if you are interested.
   * :::
   *
   * Update many tables
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateTables: Array<Table>;
  /**
   * Update an existing Term, `id` of a valid Term is necessary, all other fields are optional.
   *
   * - `description` supports markdown
   * - `linkedTagId` will override any linked term on the given tag
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  updateTerm: Term;
  /**
   * With this mutation you can add or update multiple quality checks on a single table.
   *
   * Quality checks are unique per `tableId` and `externalId` combination.
   *
   * Thus, for a given `tableId` and `externalId` pair, we only keep the latest test run.
   *
   * This means that for the provided checks, if the `runAt` value is set after the existing one, the new value will replace it. If however one test exists on those keys and has a `runAt` after the one you’re trying to insert, it will not be added.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  upsertDataQualities: Array<QualityCheck>;
  /**
   * Upsert many Lineages
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  upsertLineages: Array<Lineage>;
  /**
   * Upsert many pinned assets
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `fromDashboard.source`
   * - `fromDashboard`
   * - `fromTable.schema.database.warehouse`
   * - `fromTable.schema.database`
   * - `fromTable.schema`
   * - `fromTable`
   * - `fromTerm.parentTerm`
   * - `fromTerm`
   * - `toColumn.table.schema.database.warehouse`
   * - `toColumn.table.schema.database`
   * - `toColumn.table.schema`
   * - `toColumn.table`
   * - `toColumn`
   * - `toDashboard.source`
   * - `toDashboard`
   * - `toDashboardField.dashboard.source`
   * - `toDashboardField.dashboard`
   * - `toDashboardField`
   * - `toTable.schema.database.warehouse`
   * - `toTable.schema.database`
   * - `toTable.schema`
   * - `toTable`
   * - `toTerm.parentTerm`
   * - `toTerm`
   */
  upsertPinnedAssets: Array<EntitiesLink>;
  /**
   * Upsert a Team. If the name do not exist, then it will create a new team otherwise it will update the existing team.
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  upsertTeam: Team;
  /**
   * Add team ownership to assets in Catalog
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  upsertTeamOwners: Array<TeamOwnerEntity>;
  /**
   * Upsert many User Owners
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  upsertUserOwners: Array<OwnerEntity>;
};


export type MutationAddTeamUsersArgs = {
  data: TeamUsersInput;
};


export type MutationAttachTagsArgs = {
  data: Array<BaseTagEntityInput>;
};


export type MutationCreateColumnsArgs = {
  data: Array<CreateColumnInput>;
};


export type MutationCreateDatabasesArgs = {
  data: Array<CreateDatabaseInput>;
};


export type MutationCreateExternalLinksArgs = {
  data: Array<CreateExternalLinkInput>;
};


export type MutationCreateSchemasArgs = {
  data: Array<CreateSchemaInput>;
};


export type MutationCreateSourceArgs = {
  data: CreateSourceInput;
};


export type MutationCreateTablesArgs = {
  data: Array<CreateTableInput>;
};


export type MutationCreateTermArgs = {
  data: CreateTermInput;
};


export type MutationDeleteExternalLinksArgs = {
  data: Array<DeleteExternalLinkInput>;
};


export type MutationDeleteLineagesArgs = {
  data: Array<DeleteLineageInput>;
};


export type MutationDeleteTermArgs = {
  data: DeleteTermInput;
};


export type MutationDetachTagsArgs = {
  data: Array<BaseTagEntityInput>;
};


export type MutationRemoveDataQualitiesArgs = {
  data: RemoveQualityChecksInput;
};


export type MutationRemovePinnedAssetsArgs = {
  data: Array<EntitiesLinkInput>;
};


export type MutationRemoveTeamOwnersArgs = {
  data: TeamOwnerInput;
};


export type MutationRemoveTeamUsersArgs = {
  data: TeamUsersInput;
};


export type MutationRemoveUserOwnersArgs = {
  data: OwnerInput;
};


export type MutationUpdateColumnDescriptionsArgs = {
  data: Array<UpdateColumnDescriptionInput>;
};


export type MutationUpdateColumnsArgs = {
  data: Array<UpdateColumnInput>;
};


export type MutationUpdateColumnsMetadataArgs = {
  data: Array<UpdateColumnsMetadataInput>;
};


export type MutationUpdateDatabasesArgs = {
  data: Array<UpdateDatabaseInput>;
};


export type MutationUpdateExternalLinksArgs = {
  data: Array<UpdateExternalLinkInput>;
};


export type MutationUpdateSchemasArgs = {
  data: Array<UpdateSchemaInput>;
};


export type MutationUpdateSourceArgs = {
  data: UpdateSourceInput;
};


export type MutationUpdateTableDescriptionsArgs = {
  data: Array<UpdateTableDescriptionInput>;
};


export type MutationUpdateTablesArgs = {
  data: Array<UpdateTableInput>;
};


export type MutationUpdateTermArgs = {
  data: UpdateTermInput;
};


export type MutationUpsertDataQualitiesArgs = {
  data: UpsertQualityChecksInput;
};


export type MutationUpsertLineagesArgs = {
  data: Array<UpsertLineageInput>;
};


export type MutationUpsertPinnedAssetsArgs = {
  data: Array<EntitiesLinkInput>;
};


export type MutationUpsertTeamArgs = {
  data: UpsertTeamInput;
};


export type MutationUpsertTeamOwnersArgs = {
  data: TeamOwnerInput;
};


export type MutationUpsertUserOwnersArgs = {
  data: OwnerInput;
};

/** The origin of the job */
export type Origin =
  /** Job originated from the API */
  | 'API'
  /** Job originated from the main application */
  | 'APP'
  /** Job originated from the Dust agent */
  | 'DUST'
  /** Job originated from the chrome extension */
  | 'EXTENSION'
  /** Job originated from the Microsoft Teams extension */
  | 'MS_TEAMS'
  /** Job originated from the Slack app */
  | 'SLACK_BOT';

/** Represents a user who is designated as the owner of a dashboard, table, or term. */
export type OwnerEntity = {
  createdAt: Scalars['Timestamp']['output'];
  dashboardId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The owner label */
  ownerLabel?: Maybe<OwnerLabel>;
  ownerLabelId?: Maybe<Scalars['String']['output']>;
  tableId?: Maybe<Scalars['String']['output']>;
  termId?: Maybe<Scalars['String']['output']>;
  /** The unified user linked to the owner */
  unifiedUser: UnifiedUser;
  updatedAt: Scalars['Timestamp']['output'];
  /** The owner */
  user?: Maybe<User>;
  userId?: Maybe<Scalars['String']['output']>;
};

/** Input object expected by the query or mutation */
export type OwnerInput = {
  /** Scope by multiple target entities (dashboard, table, term) */
  targetEntities?: InputMaybe<Array<EntityTarget>>;
  /** The id of the user */
  userId: Scalars['String']['input'];
};

export type OwnerLabel = {
  id: Scalars['ID']['output'];
  /** The owner label */
  label: Scalars['String']['output'];
};

/** Pagination options for the result */
export type Pagination = {
  /** Number of entities to return per page */
  nbPerPage: Scalars['Int']['input'];
  /** Fetch entities at this page, start at page 0 */
  page?: InputMaybe<Scalars['Int']['input']>;
};

/** Represents a quality test for a table */
export type QualityCheck = {
  /** Column linked to the quality check. */
  column?: Maybe<Column>;
  /** Column id linked to the quality check. */
  columnId?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Quality check description */
  description?: Maybe<Scalars['String']['output']>;
  /** The technical quality check identifier */
  externalId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Quality check name */
  name: Scalars['String']['output'];
  /** Email of the owner of the quality check */
  ownerEmail?: Maybe<Scalars['String']['output']>;
  /** Result is different from the status, it can contain information on why the test failed */
  result?: Maybe<Scalars['String']['output']>;
  /** Time at which the quality check ran */
  runAt: Scalars['Timestamp']['output'];
  /** Source that ran the quality check */
  source: Scalars['String']['output'];
  /** Status of the quality check */
  status: QualityStatus;
  /** Table linked to the quality check. */
  table: Table;
  /** Table id linked to the quality check. */
  tableId: Scalars['String']['output'];
  /** Url of the quality check */
  url?: Maybe<Scalars['String']['output']>;
};

/** Quality check identifier */
export type QualityCheckInput = {
  /** The technical quality check identifier */
  externalId: Scalars['String']['input'];
  /** Table id the target quality check is linked to */
  tableId: Scalars['String']['input'];
};

/** The status of quality check */
export type QualityStatus =
  /** Critical quality issue detected */
  | 'ALERT'
  /** Quality check passed successfully */
  | 'SUCCESS'
  /** Non-critical quality issue detected */
  | 'WARNING';

export type Query = {
  /** Starts an AI Assistant job and returns its `jobId`. Use the [`getAiAssistantJobResult`](./get-ai-assistant-job-result.mdx) query to poll for the job result. */
  addAiAssistantJob: AddAiAssistantJobOutput;
  /** Retrieves the result of an AI Assistant job by its `jobId`. Use the [`addAiAssistantJob`](./add-ai-assistant-job.mdx) query to start a job. */
  getAiAssistantJobResult: GetAiAssistantJobResultOutput;
  /**
   * Get many column joins, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `firstColumn`
   * - `firstColumn.table`
   * - `secondColumn`
   * - `secondColumn.table`
   */
  getColumnJoins: GetColumnJoinsOutput;
  /**
   * Get many columns, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `tagEntities`
   * - `tagEntities.tag`
   */
  getColumns: GetColumnsOutput;
  /**
   * Get many dashboards, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `entityEditors`
   * - `entityEditors.sourceUser`
   * - `entityEditors.sourceUser.unifiedUser`
   * - `ownerEntities`
   * - `ownerEntities.ownerLabel`
   * - `ownerEntities.unifiedUser`
   * - `ownerEntities.user`
   * - `source`
   * - `tagEntities`
   * - `tagEntities.tag`
   * - `teamOwnerEntities`
   * - `teamOwnerEntities.ownerLabel`
   * - `teamOwnerEntities.team`
   * - `verifiedBy`
   */
  getDashboards: GetDashboardsOutput;
  /**
   * Get many Data Products, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `dashboard`
   * - `dashboard.tagEntities`
   * - `dashboard.tagEntities.tag`
   * - `table`
   * - `table.tagEntities`
   * - `table.tagEntities.tag`
   * - `term`
   * - `term.tagEntities`
   * - `term.tagEntities.tag`
   */
  getDataProducts: GetDataProductOutput;
  /**
   * Get many Quality Checks, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `column`
   * - `table`
   */
  getDataQualities: GetQualityChecksOutput;
  /**
   * Get many databases, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `warehouse`
   */
  getDatabases: GetDatabasesOutput;
  /**
   * Get many Field Lineages, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  getFieldLineages: GetFieldLineagesOutput;
  /**
   * Get many Lineages, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  getLineages: GetLineagesOutput;
  /**
   * Get many Pinned Assets, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `fromDashboard.source`
   * - `fromDashboard`
   * - `fromTable.schema.database.warehouse`
   * - `fromTable.schema.database`
   * - `fromTable.schema`
   * - `fromTable`
   * - `fromTerm.parentTerm`
   * - `fromTerm`
   * - `toColumn.table.schema.database.warehouse`
   * - `toColumn.table.schema.database`
   * - `toColumn.table.schema`
   * - `toColumn.table`
   * - `toColumn`
   * - `toDashboard.source`
   * - `toDashboard`
   * - `toDashboardField.dashboard.source`
   * - `toDashboardField.dashboard`
   * - `toDashboardField`
   * - `toTable.schema.database.warehouse`
   * - `toTable.schema.database`
   * - `toTable.schema`
   * - `toTable`
   * - `toTerm.parentTerm`
   * - `toTerm`
   */
  getPinnedAssets: GetEntitiesLinkOutput;
  /**
   * Get many schemas, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `database`
   * - `database.warehouse`
   */
  getSchemas: GetSchemasOutput;
  /**
   * Get many sources, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  getSources: GetSourcesOutput;
  /** Get queries associated with tables with scoping */
  getTableQueries: GetTableQueriesOutput;
  /**
   * Get many tables, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `entityEditors`
   * - `entityEditors.sourceUser`
   * - `entityEditors.sourceUser.unifiedUser`
   * - `externalLinks`
   * - `ownerEntities`
   * - `ownerEntities.ownerLabel`
   * - `ownerEntities.unifiedUser`
   * - `ownerEntities.user`
   * - `schema`
   * - `schema.database`
   * - `schema.database.warehouse`
   * - `tagEntities`
   * - `tagEntities.tag`
   * - `teamOwnerEntities`
   * - `teamOwnerEntities.ownerLabel`
   * - `teamOwnerEntities.team`
   * - `verifiedBy`
   */
  getTables: GetTablesOutput;
  /**
   * Get many tags, with optional scoping, sorting and pagination
   *
   *
   *
   * All tags will be retrieved, including tags on soft deleted and hidden entities. This means tags on:
   *
   * - soft deleted columns, tables, dashboards, terms and any other kind of assets
   * - hidden assets: databases, schemas, etc
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  getTags: GetTagsOutput;
  /**
   * Get many teams, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You cannot request any relation from the returned response
   */
  getTeams: Array<GetTeamsOutput>;
  /**
   * Get many Terms, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `childrenTerms`
   * - `entityEditors`
   * - `entityEditors.sourceUser`
   * - `entityEditors.sourceUser.unifiedUser`
   * - `linkedTag`
   * - `ownerEntities`
   * - `ownerEntities.ownerLabel`
   * - `ownerEntities.unifiedUser`
   * - `ownerEntities.user`
   * - `parentTerm`
   * - `tagEntities`
   * - `tagEntities.tag`
   * - `teamOwnerEntities`
   * - `teamOwnerEntities.ownerLabel`
   * - `teamOwnerEntities.team`
   * - `verifiedBy`
   */
  getTerms: GetTermsOutput;
  /**
   * Get many user, with optional scoping, sorting and pagination
   *
   * ### Allowed relations
   *
   * You can request the following relations in the response:
   *
   * - `ownerEntities`
   * - `unifiedUser`
   */
  getUsers: Array<GetUsersOutput>;
  /** Perform a semantic search on your ingested SQL queries, using natural language and return 10 relevant results */
  searchQueries: SearchQueriesOutput;
};


export type QueryAddAiAssistantJobArgs = {
  data: AddAiAssistantJobInput;
};


export type QueryGetAiAssistantJobResultArgs = {
  data: JobResultInput;
};


export type QueryGetColumnJoinsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetColumnJoinsScope>;
  sorting?: InputMaybe<Array<ColumnJoinSorting>>;
};


export type QueryGetColumnsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetColumnsScope>;
  sorting?: InputMaybe<Array<ColumnSorting>>;
};


export type QueryGetDashboardsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetDashboardsScope>;
  sorting?: InputMaybe<Array<DashboardSorting>>;
};


export type QueryGetDataProductsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetDataProductScope>;
  sorting?: InputMaybe<Array<DataProductSorting>>;
};


export type QueryGetDataQualitiesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetQualityChecksScope>;
};


export type QueryGetDatabasesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetDatabasesScope>;
};


export type QueryGetFieldLineagesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope: GetFieldLineagesScope;
  sorting?: InputMaybe<Array<FieldLineageSorting>>;
};


export type QueryGetLineagesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetLineagesScope>;
  sorting?: InputMaybe<Array<LineageSorting>>;
};


export type QueryGetPinnedAssetsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetEntitiesLinksScope>;
  sorting?: InputMaybe<Array<EntitiesLinkSorting>>;
};


export type QueryGetSchemasArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetSchemasScope>;
};


export type QueryGetSourcesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetSourcesScope>;
};


export type QueryGetTableQueriesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetTableQueriesScope>;
  sorting?: InputMaybe<Array<QuerySorting>>;
};


export type QueryGetTablesArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetTablesScope>;
  sorting?: InputMaybe<Array<TableSorting>>;
};


export type QueryGetTagsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetTagsScope>;
  sorting?: InputMaybe<Array<TagSorting>>;
};


export type QueryGetTeamsArgs = {
  pagination?: InputMaybe<Pagination>;
};


export type QueryGetTermsArgs = {
  pagination?: InputMaybe<Pagination>;
  scope?: InputMaybe<GetTermsScope>;
  sorting?: InputMaybe<Array<TermSorting>>;
};


export type QueryGetUsersArgs = {
  pagination?: InputMaybe<Pagination>;
};


export type QuerySearchQueriesArgs = {
  data: SearchQueriesInput;
  scope?: InputMaybe<SearchQueriesScope>;
};

/** Sorting options for results */
export type QuerySorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: QuerySortingKey;
};

/** The possible sort fields for queries */
export type QuerySortingKey =
  /** Sort by the query hash */
  | 'hash'
  /** Sort by the query type (SELECT or WRITE) */
  | 'queryType'
  /** Sort by the query timestamp */
  | 'timestamp';

/** The type of the query action */
export type QueryType =
  /** Read-only query that retrieves data */
  | 'SELECT'
  /** Query that modifies or writes data */
  | 'WRITE';

/** Input object expected by the query or mutation */
export type RemoveQualityChecksInput = {
  /** Array of quality checks tableId/externalId keys */
  qualityChecks: Array<QualityCheckInput>;
};

/** A schema represents a collection of tables and views. */
export type Schema = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The database linked to this schema */
  database: Database;
  /** The database ID of the schema */
  databaseId: Scalars['String']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The Catalog documentation for this schema */
  description?: Maybe<Scalars['String']['output']>;
  /** The technical schema identifier from the warehouse schema structure */
  externalId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Whether this schema is hidden */
  isHidden: Scalars['Boolean']['output'];
  /** The name of the schema */
  name: Scalars['String']['output'];
  /** Unique Catalog slug of the resource */
  slug: Scalars['String']['output'];
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** How search array filter should capture provided array values */
export type SearchArrayFilterMode =
  /** All values of the provided array must be present in the target array */
  | 'ALL'
  /** At least one value of the provided array must be present in the target array */
  | 'ANY';

/** Input object expected by the query or mutation */
export type SearchQueriesInput = {
  /** Question used to find related queries. Must be less than 256 words and 1024 characters */
  question: Scalars['String']['input'];
  /** Enforce translation of the provided question to english. Can enhance results precision, however take more time */
  translateQuestionToEnglish?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Response from the executed query or mutation */
export type SearchQueriesOutput = {
  /** Field containing the response from the executed query or mutation */
  data: Array<SearchQueryResult>;
};

/** Allow to refine selection of the results using multiple scopes. Note: all scopes are additive */
export type SearchQueriesScope = {
  /** How filter should capture provided table IDs. Default to "ALL" */
  filterMode?: InputMaybe<FilterTablesMode>;
  /** Filter queries using provided table IDs (max 10 ids) */
  tableIds: Array<Scalars['String']['input']>;
};

/** A single query result */
export type SearchQueryResult = {
  /** Query author information */
  author?: Maybe<SearchQueryResultAuthor>;
  /** Query matching provided question */
  query: Scalars['String']['output'];
  /** Table IDs used by this query */
  tableIds: Array<Scalars['String']['output']>;
};

/** Query author information */
export type SearchQueryResultAuthor = {
  /** Query author email */
  email?: Maybe<Scalars['String']['output']>;
  /** Query author name */
  name?: Maybe<Scalars['String']['output']>;
};

/** The possible sort directions : ASC/DESC, default: asc */
export type SortingDirectionEnum =
  /** Sort in ascending order */
  | 'ASC'
  /** Sort in descending order */
  | 'DESC';

/** The position of the nulls first/last, default: last */
export type SortingNullsPriority =
  /** Place null values at the beginning of sorted results */
  | 'FIRST'
  /** Place null values at the end of sorted results */
  | 'LAST';

/** A source represents a data source that can be used to create dashboards, tables, and knowledge. Each source contains metadata, documentation, and links to its related entities. */
export type Source = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The time at which the source was last ingested by service */
  lastRefreshedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The name of the source */
  name: Scalars['String']['output'];
  /** Whether the source is issued from the API or the extraction */
  origin: SourceOrigin;
  /** Unique Catalog slug of the resource */
  slug: Scalars['String']['output'];
  /** The technology of the source */
  technology: SourceTechnology;
  /** Type of the source */
  type: SourceType;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** The source origin */
export type SourceOrigin =
  /** Data pushed via the public API */
  | 'API'
  /** Data originating from a customer data source */
  | 'EXTRACTION';

export type SourceTechnology =
  | 'AMAZON_ATHENA'
  | 'ANOMALO'
  | 'BIGQUERY'
  | 'COALESCE'
  | 'COALESCE_QUALITY'
  | 'COGNOS'
  | 'CONFLUENCE'
  | 'DATABRICKS'
  | 'DBT'
  | 'DBT_TEST'
  | 'DELTALAKE'
  | 'DOMO'
  | 'DOMO_DATA'
  | 'DREMIO'
  | 'DRUID'
  | 'DYNAMODB'
  | 'EXASOL'
  | 'FIREBOLT'
  | 'FIVETRAN'
  | 'GENERIC_VISUALIZATION'
  | 'GENERIC_WAREHOUSE'
  | 'GLUE'
  | 'GREAT_EXPECTATIONS'
  | 'HIVE'
  | 'KAFKA'
  | 'KEBOOLA'
  | 'LOOKER'
  | 'LOOKER_STUDIO'
  | 'MARIADB'
  | 'METABASE'
  | 'MIXPANEL'
  | 'MODE'
  | 'MONTE_CARLO'
  | 'MS_TEAMS'
  | 'MYSQL'
  | 'NOTION'
  | 'ORACLE'
  | 'PERISCOPE'
  | 'POSTGRES'
  | 'POWERBI'
  | 'PRESTODB'
  | 'QLIK_SENSE'
  | 'REDASH'
  | 'REDSHIFT'
  | 'S3'
  | 'SALESFORCE'
  | 'SALESFORCE_REPORTING'
  | 'SIFFLET'
  | 'SIGMA'
  | 'SISENSE'
  | 'SLACK'
  | 'SNAPLOGIC'
  | 'SNOWFLAKE'
  | 'SODA'
  | 'SQLSERVER'
  | 'SSRS'
  | 'STRATEGY'
  | 'SUPERSET'
  | 'SYNAPSE'
  | 'TABLEAU'
  | 'TERADATA'
  | 'THOUGHTSPOT'
  | 'TRINO'
  | 'VERTICA'
  | 'ZOHO';

/** type of source */
export type SourceType =
  /** Source for communication and messaging systems */
  | 'COMMUNICATION'
  /** Source for knowledge management and documentation */
  | 'KNOWLEDGE'
  /** Source for data quality and validation */
  | 'QUALITY'
  /** Source for data transformation and processing */
  | 'TRANSFORMATION'
  /** Source for data visualization and reporting */
  | 'VISUALIZATION'
  /** Source for data warehouse and storage */
  | 'WAREHOUSE';

/** Represents a user imported from an external data source, including their name, email, etc. */
export type SourceUser = {
  avatarUrl?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['Timestamp']['output'];
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The optional email for this source query */
  email?: Maybe<Scalars['String']['output']>;
  /** The internal reference to this user in the source */
  externalId: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  name?: Maybe<Scalars['String']['output']>;
  /** The source user belongs to this source ID. */
  sourceId: Scalars['String']['output'];
  /** The precomputed unifiedId */
  unifiedId?: Maybe<Scalars['String']['output']>;
  /** The unified user associated to that source user. */
  unifiedUser?: Maybe<UnifiedUser>;
  updatedAt: Scalars['Timestamp']['output'];
};

/** A table represents a collection of data stored in a database. Each table contains columns, metadata, documentation, and links to its related entities. */
export type Table = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The time at which the table was deprecated */
  deprecatedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The table was deprecated by the user belonging to this ID */
  deprecatedByUserId?: Maybe<Scalars['String']['output']>;
  /** The table custom rawDescription or the external one if empty */
  description: Scalars['String']['output'];
  /** The Catalog documentation for this table, as raw text. Can contain Markdown. */
  descriptionRaw?: Maybe<Scalars['String']['output']>;
  /** Catalog description in Lexical format (see https://lexical.dev/ documentation) */
  descriptionStateLexical?: Maybe<Scalars['JSON']['output']>;
  /** The editors associated to this table */
  entityEditors: Array<EntityEditor>;
  /** The documentation from the source for this table */
  externalDescription?: Maybe<Scalars['String']['output']>;
  /** The source of the documentation for this table */
  externalDescriptionSource?: Maybe<TableExternalDescriptionSource>;
  /** The technical table identifier from the warehouse table structure */
  externalId: Scalars['String']['output'];
  /** External Links of a table */
  externalLinks?: Maybe<Array<ExternalLink>>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Whether this table has been marked as deprecated */
  isDeprecated: Scalars['Boolean']['output'];
  /** True if the description has been generated, false otherwise */
  isDescriptionGenerated?: Maybe<Scalars['Boolean']['output']>;
  /** Whether this table has been marked as verified */
  isVerified: Scalars['Boolean']['output'];
  /** The table was last described be the user belonging to this ID */
  lastDescribedByUserId?: Maybe<Scalars['String']['output']>;
  /** The last datetime at which we queried for this table freshness */
  lastQueriedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The last known datetime of refresh for this table */
  lastRefreshedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The table name */
  name: Scalars['String']['output'];
  /** The number of queries on which popularity is based */
  numberOfQueries?: Maybe<Scalars['Int']['output']>;
  /** The individual owners of the table */
  ownerEntities: Array<OwnerEntity>;
  /** ·The table popularity rated out of 1000000 */
  popularity?: Maybe<Scalars['Int']['output']>;
  /** The table belongs to this schema */
  schema: Schema;
  /** The table belongs to this schema ID */
  schemaId: Scalars['String']['output'];
  /** The Catalog slug of the table */
  slug: Scalars['String']['output'];
  /** The size of the table (in megabytes) */
  tableSize?: Maybe<Scalars['Int']['output']>;
  /** The type of table asset: e.g. view, table */
  tableType: TableType;
  /** Tag associated to this table */
  tagEntities: Array<TagEntity>;
  /** The team owners of the table */
  teamOwnerEntities: Array<TeamOwnerEntity>;
  /** The transformation source technology of this table */
  transformationSource?: Maybe<TransformationSource>;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
  /** The external url to the table */
  url?: Maybe<Scalars['String']['output']>;
  /** The time at which the table was certified */
  verifiedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The table is verified by this user */
  verifiedBy?: Maybe<User>;
  /** The table is verified by the user belonging to this ID */
  verifiedByUserId?: Maybe<Scalars['String']['output']>;
};

/** Table External Sources of documentation available */
export type TableExternalDescriptionSource =
  /** Description pushed via the public API */
  | 'API'
  /** Description generated by Catalog */
  | 'CASTOR'
  /** Description originating from coalesce */
  | 'COALESCE'
  /** Description originating from a customer warehouse */
  | 'DATABASE'
  /** Description originating from dbt */
  | 'DBT';

/** Table data related to a SQL query */
export type TableInQueryOutput = {
  /** The table id */
  id: Scalars['String']['output'];
  /** The table name */
  name: Scalars['String']['output'];
  /** The table path */
  path: Scalars['String']['output'];
};

/** A SQL query being run on the warehouse either to READ or WRITE data */
export type TableQueryOutput = {
  /** The query's author */
  author?: Maybe<Scalars['String']['output']>;
  /** The database ids */
  databaseIds: Array<Scalars['String']['output']>;
  /** The query identifier */
  hash: Scalars['ID']['output'];
  /** The query's visibility */
  isHidden: Scalars['Boolean']['output'];
  /** The query duration (in seconds) */
  queriesDuration?: Maybe<Scalars['Float']['output']>;
  /** The query's content */
  query: Scalars['String']['output'];
  /** The query's type */
  queryType: QueryType;
  /** The number of rows produced by the query */
  rowsProduced?: Maybe<Scalars['Int']['output']>;
  /** The schema ids */
  schemaIds: Array<Scalars['String']['output']>;
  /** The snowflake warehouse size used to run the query */
  snowflakeWarehouseSize?: Maybe<Scalars['String']['output']>;
  /** The source user id associated to the query */
  sourceUserId?: Maybe<Scalars['String']['output']>;
  /** The tables used in the query */
  tables: Array<TableInQueryOutput>;
  /** date of the execution of the query */
  timestamp: Scalars['Timestamp']['output'];
  /** The warehouse ids */
  warehouseIds: Array<Scalars['String']['output']>;
};

/** Sorting options for results */
export type TableSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: TableSortingKey;
};

/** The possible sort fields */
export type TableSortingKey =
  /** Sort by the level of completion */
  | 'levelOfCompletion'
  /** Sort by the table name */
  | 'name'
  /** Sort by the table name length */
  | 'nameLength'
  /** Sort by the number of owners and team owners */
  | 'ownersAndTeamOwnersCount'
  /** Sort by the table popularity */
  | 'popularity'
  /** Sort by the schema name */
  | 'schemaName';

/** The possible types for a table */
export type TableType =
  /** A Snowflake specific table type, is dynamically updated depending on some criteria */
  | 'DYNAMIC_TABLE'
  /** Table from an external source */
  | 'EXTERNAL'
  /** Standard table */
  | 'TABLE'
  /** Topic or subject-specific table */
  | 'TOPIC'
  /** Virtual table defined by a query */
  | 'VIEW';

/** A tag represents a label applied to entities (dashboards, tables, columns, etc.) to categorize and organize them. */
export type Tag = {
  /** The color given to the tag */
  color?: Maybe<Colors>;
  createdAt: Scalars['Timestamp']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The label given to the tag */
  label: Scalars['String']['output'];
  /** The ID of the term linked to tag */
  linkedTermId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog slug of the resource */
  slug: Scalars['String']['output'];
  updatedAt: Scalars['Timestamp']['output'];
};

/** A tag entity represents a tag applied to an entity (dashboard, table, column, etc.) */
export type TagEntity = {
  createdAt: Scalars['Timestamp']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Author of a tag entity */
  origin: TagEntityOrigin;
  /** The tag concerned */
  tag: Tag;
  tagId: Scalars['String']['output'];
  /** The source technology that brought the tag (for EXTERNAL tag entities only) */
  technology?: Maybe<SourceTechnology>;
  updatedAt: Scalars['Timestamp']['output'];
};

/** The possible author of a tag entity */
export type TagEntityOrigin =
  /** Tag originating for a customer source */
  | 'EXTERNAL'
  /** Tag created by Catalog */
  | 'INTERNAL'
  /** Tag created by a user */
  | 'USER';

/** Tag's target entity type */
export type TagEntityType =
  /** The column entity */
  | 'COLUMN'
  /** The dashboard entity */
  | 'DASHBOARD'
  /** The dashboard field entity */
  | 'DASHBOARD_FIELD'
  /** The table entity */
  | 'TABLE'
  /** The knowledge entity */
  | 'TERM';

/** Sorting options for results */
export type TagSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: TagSortingKey;
};

/** The possible sort fields */
export type TagSortingKey =
  /** Sort by the tag label */
  | 'label';

/** A team represents a group of users. */
export type Team = {
  /** The avatar url of the team */
  avatarUrl?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The description of the team */
  description?: Maybe<Scalars['String']['output']>;
  /** The email of the team */
  email?: Maybe<Scalars['String']['output']>;
  /** external id of the team when synchronized from an external source */
  externalId?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The name of the team */
  name: Scalars['String']['output'];
  /** The slack channel of the team (start with #) */
  slackChannel?: Maybe<Scalars['String']['output']>;
  /** The slack group of the team (start with @) */
  slackGroup?: Maybe<Scalars['String']['output']>;
  /** Indicate where the team come from: HR tool, Slack, Google groups... */
  source?: Maybe<Scalars['String']['output']>;
  /** The entities owned by the team */
  teamOwnerEntities: Array<TeamOwnerEntity>;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** A team owner entity represents a team that owns an entity (dashboard, table, knowledge). */
export type TeamOwnerEntity = {
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The owner label */
  ownerLabel?: Maybe<OwnerLabel>;
  ownerLabelId?: Maybe<Scalars['String']['output']>;
  /** The owning team */
  team: Team;
  /** the owning team id */
  teamId: Scalars['String']['output'];
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** Input object expected by the query or mutation */
export type TeamOwnerInput = {
  /** Scope by multiple tag IDs */
  targetEntities?: InputMaybe<Array<EntityTarget>>;
  /** The id of the team */
  teamId: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type TeamUsersInput = {
  /** Emails of users to add or remove from the team */
  emails: Array<Scalars['String']['input']>;
  /** The id of the team */
  id: Scalars['String']['input'];
};

/** Also known as Knowledge page, a Term is used to document your data. */
export type Term = {
  /** The children terms of this term */
  childrenTerms: Array<Term>;
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** Date and time the resource was deleted */
  deletedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The time at which the term was deprecated */
  deprecatedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The term was deprecated by the user belonging to this ID */
  deprecatedByUserId?: Maybe<Scalars['String']['output']>;
  /** The term depth - Default 0 is highest in hierarchy */
  depthLevel?: Maybe<Scalars['Int']['output']>;
  /** The term's raw description or the external one if empty */
  description: Scalars['String']['output'];
  /** The Catalog documentation for this term, as raw text */
  descriptionRaw?: Maybe<Scalars['String']['output']>;
  /** Catalog description in Lexical format (see https://lexical.dev/ documentation) */
  descriptionStateLexical?: Maybe<Scalars['JSON']['output']>;
  /** The entity editors this term points to */
  entityEditors: Array<EntityEditor>;
  /** The documentation from the external source for this term */
  externalDescription?: Maybe<Scalars['String']['output']>;
  /** The source of the documentation for this term */
  externalDescriptionSource?: Maybe<TermExternalDescriptionSource>;
  /** External ID of the term when synchronized from a source */
  externalId?: Maybe<Scalars['String']['output']>;
  /** The icon associated to the term */
  icon?: Maybe<Scalars['String']['output']>;
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** Whether this term has been marked as deprecated */
  isDeprecated: Scalars['Boolean']['output'];
  /** True if the description has been generated, false otherwise */
  isDescriptionGenerated?: Maybe<Scalars['Boolean']['output']>;
  /** Is the term verified? */
  isVerified: Scalars['Boolean']['output'];
  /** The term was last described by this user */
  lastDescribedBy?: Maybe<User>;
  /** The term was last described be the user belonging to this ID */
  lastDescribedByUserId?: Maybe<Scalars['String']['output']>;
  /** The time at which the term was last edited */
  lastEditedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The tag to which this term is linked */
  linkedTag?: Maybe<Tag>;
  /** The term name */
  name?: Maybe<Scalars['String']['output']>;
  /** The individual owners of the term */
  ownerEntities: Array<OwnerEntity>;
  /** The parent term of this term */
  parentTerm?: Maybe<Term>;
  /** The id of the parent term of this term */
  parentTermId?: Maybe<Scalars['String']['output']>;
  slug: Scalars['String']['output'];
  /** The tag entities belonging to this term */
  tagEntities: Array<TagEntity>;
  /** The team owners of the term */
  teamOwnerEntities: Array<TeamOwnerEntity>;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
  /** The term's url in the external source */
  url?: Maybe<Scalars['String']['output']>;
  /** The time at which the term was verified */
  verifiedAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The term is verified by this user */
  verifiedBy?: Maybe<User>;
  /** The term is verified by the user belonging to this ID */
  verifiedByUserId?: Maybe<Scalars['String']['output']>;
};

/** Term available external documentation sources */
export type TermExternalDescriptionSource =
  /** Description generated by Catalog */
  | 'CASTOR'
  /** Description originating from a customer knowledge base */
  | 'KNOWLEDGE';

/** Sorting options for results */
export type TermSorting = {
  /** The direction to sort the results: by `ASC` or `DESC` */
  direction?: InputMaybe<SortingDirectionEnum>;
  /** The position of null values in the results: `FIRST` or `LAST` */
  nullsPriority?: InputMaybe<SortingNullsPriority>;
  /** Available attributes to sort the results by */
  sortingKey: TermSortingKey;
};

/** The possible sort fields */
export type TermSortingKey =
  /** Sort by the term name */
  | 'name'
  /** Sort by the number of owners and team owners */
  | 'ownersAndTeamOwnersCount';

/** Transformation source technologies availables */
export type TransformationSource =
  /** Description originating from coalesce */
  | 'COALESCE'
  /** Description originating from dbt */
  | 'DBT';

/** A unified user represents a user that is associated with a Catalog user and a source user. It is used to manage the user across the different systems. */
export type UnifiedUser = {
  avatarUrl?: Maybe<Scalars['String']['output']>;
  createdAt?: Maybe<Scalars['Timestamp']['output']>;
  /** The email of the unified user */
  email: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['String']['output'];
  /** The name of the unified user */
  name?: Maybe<Scalars['String']['output']>;
  /** The role of the unified user in the account */
  role?: Maybe<UserRole>;
  status?: Maybe<UserStatus>;
  /** List of team id where the unified user is present */
  teamIds: Array<Scalars['String']['output']>;
  /** The unified user is associated to this Catalog user ID */
  userId?: Maybe<Scalars['String']['output']>;
};

/** Input object expected by the query or mutation */
export type UpdateColumnDescriptionInput = {
  /** The external description for this column */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** the id of the column we want to update */
  id: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type UpdateColumnInput = {
  /** The data type for the column. For example: INTEGER */
  dataType?: InputMaybe<Scalars['String']['input']>;
  /** If present, indicates when the column was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The external documentation for this column */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** The technical column identifier from the warehouse structure */
  externalId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the column you are looking for */
  id: Scalars['String']['input'];
  /** True if this column is nullable */
  isNullable?: InputMaybe<Scalars['Boolean']['input']>;
  /** The PII status to apply to the column */
  isPii?: InputMaybe<Scalars['Boolean']['input']>;
  /** The PrimaryKey status to apply to the column */
  isPrimaryKey?: InputMaybe<Scalars['Boolean']['input']>;
  /** The column name */
  name?: InputMaybe<Scalars['String']['input']>;
  /** The original column position index as imported from the source warehouse */
  sourceOrder?: InputMaybe<Scalars['Int']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateColumnsMetadataInput = {
  /** The user documentation for this column */
  descriptionRaw?: InputMaybe<Scalars['String']['input']>;
  /** The external description for this column */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** the id of the column we want to update */
  id: Scalars['String']['input'];
  /** True if this column is PII related */
  isPii?: InputMaybe<Scalars['Boolean']['input']>;
  /** True if this column is a primary key */
  isPrimaryKey?: InputMaybe<Scalars['Boolean']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateDatabaseInput = {
  /** If present, indicates when the database was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The Catalog documentation for this database (max length: 500) */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The technical database identifier from the warehouse structure */
  externalId?: InputMaybe<Scalars['String']['input']>;
  /** The database id */
  id: Scalars['String']['input'];
  /** Indicate if the database should be hidden */
  isHidden?: InputMaybe<Scalars['Boolean']['input']>;
  /** The database name */
  name?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateExternalLinkInput = {
  /** the id of the link to update */
  id: Scalars['String']['input'];
  /** the url of the link */
  url?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateSchemaInput = {
  /** If present, indicates when the schema was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The Catalog documentation for this schema */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The technical schema identifier from the warehouse schema structure */
  externalId?: InputMaybe<Scalars['String']['input']>;
  /** The schema id */
  id: Scalars['String']['input'];
  /** Whether the schema should be hidden */
  isHidden?: InputMaybe<Scalars['Boolean']['input']>;
  /** The schema name */
  name?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateSourceInput = {
  /** The time at which the source was soft deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The id of the source to update */
  id: Scalars['String']['input'];
  /** The name of the source to update */
  name?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateTableDescriptionInput = {
  /** The external description for the table */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** The table id */
  id: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type UpdateTableInput = {
  /** If present, indicates when the table was deleted */
  deletedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The documentation from the source for this table */
  externalDescription?: InputMaybe<Scalars['String']['input']>;
  /** The technical table identifier from the warehouse table structure */
  externalId?: InputMaybe<Scalars['String']['input']>;
  /** The table id */
  id: Scalars['String']['input'];
  /** The last known datetime of refresh for this table */
  lastRefreshedAt?: InputMaybe<Scalars['DateTime']['input']>;
  /** The table name */
  name?: InputMaybe<Scalars['String']['input']>;
  /** The number of queries on which popularity is based */
  numberOfQueries?: InputMaybe<Scalars['Int']['input']>;
  /** The table popularity rated out of 1000000 */
  popularity?: InputMaybe<Scalars['Int']['input']>;
  /** The size of the table (in megabytes) */
  tableSize?: InputMaybe<Scalars['Int']['input']>;
  /** The type of this table */
  tableType?: InputMaybe<TableType>;
  /** The external url to the table */
  url?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpdateTermInput = {
  /** Description for the term, supports markdown */
  description?: InputMaybe<Scalars['String']['input']>;
  /** ID of the term to update */
  id: Scalars['String']['input'];
  /** ID of the tag to link to the term, remove by sending null */
  linkedTagId?: InputMaybe<Scalars['String']['input']>;
  /** Name of the term */
  name?: InputMaybe<Scalars['String']['input']>;
  /** ID of the parent term, null will set as root term */
  parentTermId?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpsertLineageInput = {
  /** The id of the children dashboard of the lineage */
  childDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the children table of the lineage */
  childTableId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the parent dashboard of the lineage */
  parentDashboardId?: InputMaybe<Scalars['String']['input']>;
  /** The id of the parent table of the lineage */
  parentTableId?: InputMaybe<Scalars['String']['input']>;
};

/** Input object expected by the query or mutation */
export type UpsertQualityChecksInput = {
  /** Array of quality checks */
  qualityChecks: Array<BaseQualityCheckInput>;
  /** Table id linked to quality checks array. */
  tableId: Scalars['String']['input'];
};

/** Input object expected by the query or mutation */
export type UpsertTeamInput = {
  /** The description of the team */
  description?: InputMaybe<Scalars['String']['input']>;
  /** The email of the team */
  email?: InputMaybe<Scalars['String']['input']>;
  /** The name of the team: unique across your account */
  name: Scalars['String']['input'];
  /** The slack channel of the team (start with #) */
  slackChannel?: InputMaybe<Scalars['String']['input']>;
  /** The slack group of the team (start with @) */
  slackGroup?: InputMaybe<Scalars['String']['input']>;
  /** Private slack channel of the team. Used for notifications (only available for admin or service).  Don't forget to invite the slack bot into this channel! */
  storedPrivateSlackChannel?: InputMaybe<Scalars['String']['input']>;
};

/** The user entity that authenticate in the Catalog application */
export type User = {
  /** The avatar url of the user */
  avatarUrl?: Maybe<Scalars['String']['output']>;
  /** Date and time the resource was created */
  createdAt: Scalars['Timestamp']['output'];
  /** The email of the user */
  email: Scalars['String']['output'];
  /** external id of the user when synchronized from an external source */
  externalId?: Maybe<Scalars['String']['output']>;
  /** The first name of the user */
  firstName: Scalars['String']['output'];
  /** The full name of the user */
  fullName: Scalars['String']['output'];
  /** Unique Catalog identifier of the resource */
  id: Scalars['ID']['output'];
  /** The last name of the user */
  lastName: Scalars['String']['output'];
  /** The entities owned by the user */
  ownerEntities: Array<OwnerEntity>;
  /** The role of the user in the account */
  role: UserRole;
  /** The status of the user in the account */
  status: UserStatus;
  /** The precomputed unifiedId */
  unifiedId: Scalars['String']['output'];
  /** The unified user linked to that user. */
  unifiedUser: UnifiedUser;
  /** Date and time the resource was last updated */
  updatedAt: Scalars['Timestamp']['output'];
};

/** The role of a Catalog user restricting access to Catalog features. */
export type UserRole =
  /** Full administrative access with all privileges */
  | 'ADMIN'
  /** Can view and contribute to content */
  | 'CONTRIBUTOR'
  /** Partial administrative access */
  | 'STEWARD'
  /** Read-only access to content */
  | 'VIEWER';

/** The current status of the user (e.g. pending, active) */
export type UserStatus =
  | 'ACTIVATED'
  | 'PENDING'
  | 'SUSPENDED';
