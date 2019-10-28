import { Connection, QueryRunner, SelectQueryBuilder, getConnection, OrderByCondition } from "typeorm"
import { RelationMetadata } from "typeorm/metadata/RelationMetadata"
import * as _ from "lodash"

type ScopePreType<E> = Pick<E, { [k in keyof E]: E[k] extends Function | object ? k : never }[keyof E]>
type ScopeType<E, R> = {
    [k in keyof ScopePreType<E>]: E[k] extends (q: Query<E>, ...args: infer A) => void ? (...args: A) => Query<R> : E[k] extends (infer A)[] ? () => ScopeType<A, R> : E[k] extends object ? () => ScopeType<E[k], R> : never
} & {
    pin(fn: (query: ScopeType<E, E>) => void): Query<R>,
    pinx<E2>(type: { new(): E2 }, alias: string, fn: (query: ScopeType<E2, E2>) => void): Query<R>,
}

type MetaQueryPreType<E> = Pick<E, { [k in keyof E]: E[k] extends Function ? never : k }[keyof E]>
type MetaQueryType<E, R> = {
    [k in keyof MetaQueryPreType<E>]: E[k] extends (q: Query<any>, ...args: infer A) => void ? never : (E[k] extends Function ? never : E[k] extends (infer A)[] ? () => MetaQueryType<A, R> : E[k] extends object ? () => MetaQueryType<E[k], R> : (...args) => Query<R>)
} & {
    endJoin(): Query<R>,
    as(alias: string): MetaQueryType<E, R>
    endAlias(): Query<R>,
    pin(fn: (query: MetaQueryType<E, E>) => void): Query<R>,
    pinx<E2>(type: { new(): E2 }, alias: string, fn: (query: MetaQueryType<E2, E2>) => void): Query<R>,
}
type QueryOptionsType = { connection?: Connection, alias?: string, queryRunner?: QueryRunner }

export class Query<E> {
    protected options: QueryOptionsType
    protected queryBuilder: SelectQueryBuilder<E>
    protected proxy

    protected aliases = {} // eg.: {'t0': {'relationName': 't1'}}

    protected aliasIndex = 0 // increasing at each new alias
    protected paramIndex = 0 // increasing at each new param

    protected joins = [] // eg.: ['t0.relationName', 't1.otherRelation']
    protected relations = {} // relations by 'alias'

    protected relationsQueue: string[] // relations in current operation like 'join'
    protected aliasQueue: string[] // alias in current operation like 'join'

    protected operation = '' // 'select', 'orderBy' etc
    protected currentAlias: string // eg.: join().relationName() -> currentAlias = 't1'
    protected initialsAliases: string[]

    protected lastOrderBy: string
    protected lastOperator: string
    protected lastField: string // saved in operations like where

    constructor(type: (new () => E), options: QueryOptionsType = {}) {
        // alias for 'this' to be called inside 'proxy'
        const self = this

        // adjust options
        this.options = options
        options.connection = options.connection || getConnection()
        options.alias = options.alias || 't0' //type.name.charAt(0).toLowerCase() + type.name.substr(1)

        // adjust alias
        this.currentAlias = options.alias
        this.initialsAliases = [options.alias]
        this.aliases[options.alias] = {}
        this.aliasQueue = [options.alias]

        // create SelectQueryBuilder
        this.queryBuilder = options.connection.getRepository(type).createQueryBuilder(options.alias, options.queryRunner)

        // use metadata to inspect entity
        const metadata = options.connection.getMetadata(type)

        // save relations
        self.relations[options.alias] = {}
        for (let relation of metadata.ownRelations) {
            self.relations[options.alias][relation.propertyName] = relation
        }

        return this.proxy = new Proxy(this, {
            get: function (target, name, receiver) {
                const nameString = String(name)

                // if method exists -> return it
                if (self[nameString]) {
                    return self[nameString]
                }

                // if is a relation
                if (self.relations[self.currentAlias][nameString]) {

                    const oldAlias = self.currentAlias

                    // check if this path has alias
                    // if not, set new alias and relations for it
                    self.currentAlias = self.aliases[oldAlias][nameString]

                    if (!self.currentAlias) {
                        self.currentAlias = self.aliases[oldAlias][nameString] = self.nextAlias()
                        self.aliases[self.currentAlias] = {}

                        // search current relation
                        const relation: RelationMetadata = self.relations[oldAlias][nameString]
                        const metadata = options.connection.getMetadata(relation.type)

                        // add all relations to current relation
                        self.relations[self.currentAlias] = {}
                        for (let relatedRelation of metadata.ownRelations) {
                            self.relations[self.currentAlias][relatedRelation.propertyName] = relatedRelation
                        }
                    }

                    // store nameAliased in relations queue
                    self.relationsQueue.push(nameString)
                    self.aliasQueue.push(self.currentAlias)

                    return () => self.proxy
                }

                // process 'select'
                if (['select', 'addSelect'].includes(self.operation)) {
                    return () => {
                        // call 'select'
                        self.queryBuilder[self.operation](self.aliased(nameString))

                        // clear operation params
                        self.clearOperationParams()

                        // return proxy
                        return self.proxy
                    }
                }

                // process 'where' or 'having'
                if (['where', 'andWhere', 'orWhere', 'having', 'andHaving', 'orHaving'].includes(self.operation)) {
                    return (op?, value?) => {
                        const nameAliased = self.aliased(nameString)

                        if (op && (value || value === null)) {

                            // call 'filter'
                            const param = self.nextParam()
                            if (value === null) {
                                self.queryBuilder[self.operation](`${nameAliased} ${op} null`)

                            } else {
                                self.queryBuilder[self.operation](`${nameAliased} ${op} :${param}`, { [param]: value })
                            }

                            // clear operation params
                            self.clearOperationParams()

                        } else if (self.lastField) {
                            // call 'filter'
                            self.queryBuilder[self.operation](`${self.lastField} ${self.lastOperator} ${nameAliased}`)

                            // clear operation params
                            self.clearOperationParams()

                        } else {
                            // save field and operator for later use
                            self.lastField = nameAliased
                            self.lastOperator = op
                        }

                        // return proxy
                        return self.proxy
                    }
                }

                // process 'orderBy'
                if (['orderBy', 'addOrderBy'].includes(self.operation)) {
                    return (order?: string, nulls?: string) => {
                        // adjust order
                        if (order) order = order.toUpperCase()
                        if (nulls) nulls = nulls.toUpperCase()

                        // call 'orderBy'
                        const nameAliased = self.aliased(nameString)
                        self.queryBuilder[self.operation](nameAliased, order, nulls)

                        // save it for later use
                        self.lastOrderBy = nameAliased

                        // clear operation params
                        self.clearOperationParams()

                        // return proxy
                        return self.proxy
                    }
                }

                // process 'scope'
                if (self.operation === 'scope') {
                    return (...args) => {
                        new type()[nameString](self.proxy, ...args)
                        return self.proxy
                    }
                }

                return () => self.proxy
            }
        })
    }

    protected nextAlias() {
        this.aliasIndex++
        return 't' + this.aliasIndex
    }

    protected nextParam() {
        return 'p' + this.paramIndex++
    }

    protected aliased(name: string) {
        return this.currentAlias + '.' + name
    }

    protected clearOperationParams() {
        this.relationsQueue = [];
        this.currentAlias = _.last(this.initialsAliases);
        this.aliasQueue = [this.currentAlias]
        this.lastField = null
        this.lastOperator = null
    }

    protected metaQuery() {
        return this.proxy as MetaQueryType<E, E>
    }

    scope() {
        this.operation = 'scope'

        return this.proxy as ScopeType<E, E>
    }

    select() {
        this.operation = 'select'

        return this.metaQuery()
    }

    addSelect() {
        this.operation = 'addSelect'

        return this.metaQuery()
    }

    and() {
        if (this.operation && this.operation.toLowerCase().indexOf('join') === -1) {
            let prefix = 'add'
            if (this.operation.toLowerCase().endsWith('where')) prefix = 'and'
            
            // remove old prefixes
            let operationWithoutPrefix = this.operation
            for (let oldPrefix of ['and', 'add', 'or']) {
                operationWithoutPrefix = _.trimStart(operationWithoutPrefix, oldPrefix)
            }

            // assign new operation
            this.operation = prefix + _.upperFirst(operationWithoutPrefix)
        }

        this.clearOperationParams()
        return this.metaQuery()
    }

    or() {
        const isWhere = this.operation.toLowerCase().endsWith('where')
        const isHaving = this.operation.toLowerCase().endsWith('having')

        if (this.operation && (isWhere || isHaving)) {
            this.operation = isWhere ? 'orWhere' : 'orHaving'
        }

        this.clearOperationParams()
        return this.metaQuery()
    }

    ref() {
        return this.metaQuery()
    }

    join() {
        this.operation = 'innerJoin'
        return this.metaQuery()
    }

    joinAndSelect() {
        this.operation = 'innerJoinAndSelect'
        return this.metaQuery()
    }

    innerJoin() {
        return this.join()
    }

    innerJoinAndSelect() {
        return this.joinAndSelect()
    }

    leftJoin() {
        this.operation = 'leftJoin'
        return this.metaQuery()
    }

    leftJoinAndSelect() {
        this.operation = 'leftJoinAndSelect'
        return this.metaQuery()
    }

    endJoin() {

        if (['innerJoin', 'innerJoinAndSelect', 'leftJoin', 'leftJoinAndSelect'].includes(this.operation)) {
            let parentAlias = _.last(this.initialsAliases)

            for (let relationName of this.relationsQueue) {
                const aliasAndRelation = parentAlias + '.' + relationName
                const relationAlias = this.aliases[parentAlias][relationName]

                // join if not joined before
                if (!this.joins.includes(aliasAndRelation)) {
                    this.queryBuilder[this.operation](aliasAndRelation, relationAlias)

                    // save this join to avoid repetition
                    this.joins.push(aliasAndRelation)
                }

                parentAlias = relationAlias
            }

            this.clearOperationParams()
        }

        return this.proxy as this
    }

    alias() {
        this.operation = 'alias'
        return this.metaQuery()
    }

    as(alias: string) {
        const oldAlias = this.currentAlias

        // set new alias
        this.currentAlias = alias

        // adjust aliases map
        this.aliases[_.nth(this.aliasQueue, -2)][_.last(this.relationsQueue)] = alias
        this.aliases[alias] = this.aliases[oldAlias] || {}
        delete this.aliases[oldAlias]

        // adjust alias queue
        this.aliasQueue.pop()
        this.aliasQueue.push(alias)

        // set new alias to relations map and delete old alias
        this.relations[alias] = this.relations[oldAlias]
        delete this.relations[oldAlias]

        return this.metaQuery()
    }

    endAlias() {
        this.clearOperationParams()
        return this.proxy as this
    }

    pin(fn: (query) => void) {
        // pin alias
        this.initialsAliases.push(this.currentAlias)
        this.aliasQueue = [this.currentAlias]
        this.lastField = null
        this.lastOperator = null

        // call lambda
        fn(this.proxy)

        // unpin alias
        if (this.initialsAliases.length > 1) this.initialsAliases.pop()

        this.clearOperationParams()
        return this.proxy
    }

    where() {
        this.operation = 'where'
        return this.metaQuery()
    }

    andWhere() {
        this.operation = 'andWhere'
        return this.metaQuery()
    }

    orWhere() {
        this.operation = 'orWhere'
        return this.metaQuery()
    }

    having() {
        this.operation = 'having'
        return this.metaQuery()
    }

    andHaving() {
        this.operation = 'andHaving'
        return this.metaQuery()
    }

    orHaving() {
        this.operation = 'orHaving'
        return this.metaQuery()
    }

    orderBy() {
        this.operation = 'orderBy'
        return this.metaQuery()
    }

    addOrderBy() {
        this.operation = 'addOrderBy'
        return this.metaQuery()
    }

    protected changeOrderBy(options: { order?: 'ASC' | 'DESC', nulls?: 'NULLS FIRST' | 'NULLS LAST' }) {

        // fetch last orderBy
        const allOrderBys = this.queryBuilder.expressionMap.allOrderBys
        let orderBy = allOrderBys[this.lastOrderBy] as any

        // if exists
        if (orderBy) {
            // convert to object if is string
            if (typeof orderBy === 'string') orderBy = { order: orderBy }

            // create new orderBy based on existent and argument options
            const newOrderBy = _.extend({}, orderBy, options)

            // if new orderBy does not have 'nulls', set new orderBy as string, else as a complete object
            allOrderBys[this.lastOrderBy] = newOrderBy.nulls ? newOrderBy : newOrderBy.order
        }

        return this.proxy as this
    }

    asc() {
        return this.changeOrderBy({ order: 'ASC' })
    }

    desc() {
        return this.changeOrderBy({ order: 'DESC' })
    }

    nullsFirst() {
        return this.changeOrderBy({ nulls: 'NULLS FIRST' })
    }

    nullsLast() {
        return this.changeOrderBy({ nulls: 'NULLS LAST' })
    }

    limit(value: number) {
        this.queryBuilder.limit(value)
        return this.proxy as this
    }

    async getOne() {
        return await this.queryBuilder.getOne() as E
    }

    async getMany() {
        return await this.queryBuilder.getMany() as E[]
    }

    qb(fn: (qb: SelectQueryBuilder<E>) => void) {
        fn(this.queryBuilder)
        return this.proxy as this
    }

    getSql() {
        return this.queryBuilder.getSql()
    }

    printSql(params = false) {

        if (params) {
            console.log(this.queryBuilder.getSql(), _.valuesIn(this.queryBuilder.getParameters()))

        } else {
            console.log(this.queryBuilder.getSql())
        }

        return this.proxy as this
    }
}