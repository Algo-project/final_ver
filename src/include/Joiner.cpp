#include "Joiner.h"
#include <sstream>
#include <cmath>


/* Some Helpers */

static void mapper_insert(SelPosMap &mapper, Id_t relID, 
        IdList &seled_cols, unsigned partner);
static void configure_hashjoin(Relation *r, SelectInfo &sel, int ptsize,
        HashJoin *root);
static int analyze_input_join(IdSet& usedRelations,
        SelectInfo& leftInfo,SelectInfo& rightInfo);
static void column_update(SelPosMap& mapper,unsigned partner_size);
static unsigned vector_index(IdList &vec, unsigned element);

struct JoinInput
{
    Operator *input;
    unsigned cols;
    Id_t selCol;

    JoinInput(Operator *child):child_(child)
    {
        input = NULL;
        cols = child->GetColumnCount();
        selCol = -1;
    }

    ~JoinInput(){}

    void Configure(SelPosMap &mapper, SelectInfo &sinfo, QueryInfo &qinfo, unsigned partner)
    {
        IdList return_needed;
        for(size_t i=0;i<child_->GetColumnCount();++i)
            return_needed.push_back(i);

        mapper_insert(mapper,sinfo.binding,return_needed,partner);

        cols = return_needed.size();
        input = Joiner::_addFilter(sinfo.binding, child_, qinfo);
        selCol = mapper[sinfo.binding][sinfo.colId];
    }

    private:
        Operator *child_;
};


/* Class Joiner */

Joiner::Joiner(const Database *db):relations_(&(db->relations))
{
    auto size = db->Size();
    for(size_t i=0;i<size;i++)
        catalog_.push_back(db->relations[i]->GetCatalog());
}

Joiner::~Joiner()
{
    /* do NOT delete catalog_[i] */
}

Operator *Joiner::_addScan(IdSet &used_relations,
                    SelectInfo &sinfo, QueryInfo &qinfo)
{
    used_relations.insert(sinfo.binding);
    Scan *newScan = new Scan(*(*relations_)[sinfo.relId], sinfo.binding);
    return newScan;
}

Operator *Joiner::_addFilter(Id_t sel_binding,
                    Operator *child, QueryInfo &qinfo)
{
    std::vector<FilterInfo> filters;
    for(auto &flt : qinfo.filters)
    {
        if(flt.filterColumn.binding == sel_binding)   
            filters.push_back(flt);
    }
    if(filters.size() == 0) return child;
    
    Operator *prev_op = child, *curr_op;
    for(auto &flt:filters)
    {
        curr_op = new Filter(std::move(prev_op), flt, prev_op->GetColumnCount());
        prev_op = curr_op;
    }
    
    return curr_op;
}



std::string Joiner::join1(QueryInfo &&info)
{
    static const int HTSIZE = 1<<10;
    IdSet used_rel;
    SelPosMap mapper;
    Operator *root;
    PredicateInfo &first_join = info.predicates[0];
    std::vector<unsigned> return_needed;

    uint32_t cleft, cright;
    uint32_t left_proj_colId,right_proj_colId;
    Operator *push_fil_l, *push_fil_r;

    Operator *left = _addScan(used_rel, first_join.left, info);
    Operator *right = _addScan(used_rel, first_join.right, info);

    /* Init left */
    {
        auto &sel = first_join.left;
        for(unsigned i=0;i<left->GetColumnCount();i++)
            return_needed.push_back(i);
        mapper_insert(mapper, sel.binding, return_needed, 0);
        push_fil_l = _addFilter(sel.binding, left, info);
        cleft = return_needed.size();
        left_proj_colId = vector_index(return_needed, sel.colId);
        return_needed.clear();
    }

    /* Init right */
    {
        auto &sel = first_join.right;
        for(unsigned i=0;i<right->GetColumnCount();i++)
            return_needed.push_back(i);
        cright = return_needed.size();
        right_proj_colId = vector_index(return_needed, sel.colId);
        push_fil_r = _addFilter(sel.binding, right, info);
        column_update(mapper, cright);
        mapper_insert(mapper, sel.binding, return_needed, 0);
        return_needed.clear();
    }


    root = new HashJoin(push_fil_l, cleft, left_proj_colId,
            push_fil_r, cright, right_proj_colId,
            HTSIZE);

    Relation *r = (*relations_)[first_join.right.relId];
    configure_hashjoin(r, first_join.right, HTSIZE, (HashJoin*)root);

    for(size_t i=1;i!=info.predicates.size();i++)
    {
        PredicateInfo &pinfo = info.predicates[i];
        SelectInfo &left_info = pinfo.left,
                   &right_info = pinfo.right;
        Operator *left, *right, *center;
        auto situation = analyze_input_join(used_rel,left_info,right_info);
        if(situation == 0)      /* left already used */
        {
            left = std::move(root);
            /* Init right */
            right = _addScan(used_rel, right_info, info);
            for(unsigned i = 0;i<right->GetColumnCount();i++)
                return_needed.push_back(i);
            cright = return_needed.size();
            right_proj_colId = vector_index(return_needed, right_info.colId);
            push_fil_r = _addFilter(right_info.binding, right, info);

            root = new HashJoin(left, left->GetColumnCount(), mapper[left_info.binding][left_info.colId],
                    std::move(push_fil_r), cright, right_proj_colId,
                    HTSIZE);

            column_update(mapper, right->GetColumnCount());
            mapper_insert(mapper, right_info.binding, return_needed, 0);

            Relation *r = (*relations_)[right_info.relId];
            configure_hashjoin(r, right_info, HTSIZE, (HashJoin*)root);
            return_needed.clear();
        }
        else if(situation == 1) /* right already used */
        {
            left = _addScan(used_rel, left_info, info);
            right = std::move(root);

            /* Init Left */
            for(unsigned i=0;i<left->GetColumnCount();i++)
                return_needed.push_back(i);
            cleft = return_needed.size();
            push_fil_l = _addFilter(left_info.binding, left, info);
            mapper_insert(mapper, left_info.binding, return_needed, right->GetColumnCount());
            left_proj_colId = vector_index(return_needed, left_info.colId);
            root = new HashJoin(std::move(push_fil_l), cleft, left_proj_colId,
                    std::move(right), right->GetColumnCount(), mapper[right_info.binding][right_info.colId],
                    HTSIZE);

            Relation *r = (*relations_)[left_info.relId];
            configure_hashjoin(r, left_info, HTSIZE, (HashJoin*)root);
            return_needed.clear();
        }
        else if(situation == 2) /* self join */
        {
            center = std::move(root);
            auto c1 = mapper[left_info.binding][left_info.colId],
                 c2 = mapper[right_info.binding][right_info.colId];
            root = new SelfJoin(std::move(center), c1, c2, center->GetColumnCount());
        }
        else    /* delay the join */
        {
            info.predicates.push_back(pinfo);
        }
    }
    
    IdList proj_mapper;
    for(auto &proj : info.selections)
        proj_mapper.push_back(mapper[proj.binding][proj.colId]);

    Projection *proj = new Projection(root, proj_mapper);
    CheckSum checksum(proj);

    ReColMap bd;
    Operator *newRoot = checksum.ProjectionPass(bd);

    std::vector<uint64_t *> results;
    newRoot->Open();
    newRoot->Next(&results);


    std::stringstream out;
    for(unsigned i=0;i<info.results.size();i++)
    {
        uint64_t res = (uint64_t)results[info.results[i]];
        if(res)
            out<<std::to_string(res);
        else
            out<<"NULL";
        if(i!=info.results.size()-1) out<<" ";
    }


    newRoot->Close();
    return out.str();
}




/* Struct JoinInput */

static void mapper_insert(SelPosMap &mapper, Id_t relId, IdList &seled_col,
                unsigned partner)
{
    for(size_t i=0;i<seled_col.size();++i)
        mapper[relId][seled_col[i]] = i+partner;
}


static void configure_hashjoin(Relation *r, SelectInfo &sel, int ptsize, HashJoin *root)
{
    int numrows = r->num_rows();
    uint64_t maxval = r->GetCatalog()[sel.colId * 3 + 2];
    int partsize = ptsize;
    int numparts = numrows / partsize;
    if(numparts < 128) numparts = 128;
    int rbits = ((int)(std::log2(numparts))) + 1;
    int rb1 = rbits/2;
    int rb2 = rbits - rb1;
    int first_bit = ((int)(std::log2(maxval))) - rbits;
    if(first_bit < 3) first_bit = 3;
    root->Configure(rb1,rb2,first_bit,NULL,NULL,NULL,NULL);
}

static int analyze_input_join(IdSet& usedRelations,SelectInfo& leftInfo,SelectInfo& rightInfo)
{
	bool usedLeft=usedRelations.count(leftInfo.binding);
	bool usedRight=usedRelations.count(rightInfo.binding);

	if (usedLeft^usedRight)
		return usedLeft?0:1;
	if (usedLeft&&usedRight)
		return 2;
	return 3;
}

static void column_update(SelPosMap& mapper,unsigned partner_size){
	for(auto iter = mapper.begin(); iter != mapper.end(); ++iter){
        for(auto iter2 = (iter->second).begin(); iter2 != (iter->second).end(); ++iter2){
        	//std::cout<<"UPDATER ID"<<iter->first<<std::endl;
			//std::cout<<"FROM "<<iter2->second;
        	iter2->second += partner_size;
        	//std::cout<<"TO "<<iter2->second<<std::endl;
        }
    }
}


static unsigned vector_index(std::vector<unsigned> &vec , unsigned element){
	for(int i=0 ; i< vec.size(); i++){
		if(vec[i] == element){
			return i;
		}
	}
	return -1;
}

using namespace std;
std::string Joiner::join(QueryInfo&& qinfo){
	//Build the plan of execution of the query here

	set<unsigned> used_rel;
	//RelId -> InitColId, ProjectedColId

	std::unordered_map<unsigned,std::unordered_map<unsigned,unsigned>> mapper; 

	Operator* root;
	PredicateInfo& first_join = qinfo.predicates[0];
	uint32_t cleft;
	unsigned left_proj_colId;
	uint32_t cright;
	unsigned right_proj_colId;
	std::vector<unsigned> return_needed;
	Operator* push_fil_l;
	Operator* push_fil_r;

	//Add scan will return a Scan or a Select of a Scan if there is matching filter
	//with the relation that is being scanned.
	Operator* left = _addScan(used_rel,first_join.left,qinfo);
	Operator* right = _addScan(used_rel,first_join.right,qinfo);

	//Init the root of the left deep plan tree

	//TODO::Maybe we can do it more efficiently

	//find_needed(qinfo,first_join.left,return_needed);
	for (unsigned i = 0; i < left->GetColumnCount(); i++)
		return_needed.push_back(i);

	//Operator* push_proj_l = new Projection(move(left),return_needed);
	//push_proj_l->Open();

    /* do left */
	mapper_insert(mapper,first_join.left.binding,return_needed,0);

	//Do the selections


	push_fil_l = _addFilter(first_join.left.binding,left,qinfo);

	cleft = return_needed.size();
	left_proj_colId = vector_index(return_needed,first_join.left.colId);


	//Clear return_needed buffer
	return_needed.clear();

    /* do right */
	for (unsigned i = 0; i < right->GetColumnCount(); i++)
		return_needed.push_back(i);

	cright = return_needed.size();
	right_proj_colId = vector_index(return_needed,first_join.right.colId);
	
	push_fil_r = _addFilter(first_join.right.binding,right,qinfo);

	column_update(mapper,cright);

	//std::cout<<"LeftId "<<first_join.left.relId <<"RightId "<<first_join.right.relId<<std::endl;
	mapper_insert(mapper,first_join.right.binding,return_needed,0);

	return_needed.clear();	

	root = new HashJoin(push_fil_l,cleft,left_proj_colId,push_fil_r,cright,right_proj_colId,1 << 10);

	//std::cerr<<"First Join....."<<std::endl;
	//std::cout<<"Root1"<<root<<std::endl;

    /* configure first join */
	{
	int numrows = catalog_[first_join.right.relId][0];
	uint64_t maxval = catalog_[first_join.right.relId][2+(first_join.right.colId*3)+2];

    fprintf(stderr,"maxval: %zu, numrows: %d, test: %zu\n",
            maxval, numrows, catalog_[first_join.right.relId][2]);

	int partsize = 1 << 10;
	int numparts = numrows / partsize;
	if (numparts < 128)
		numparts = 128;
	int rbits = ((int) (log2(numparts))) + 1;
	int rb1 = rbits/2;
	int rb2 = rbits-rb1;
	int first_bit = ((int) log2(maxval)) - rbits;
	if (first_bit < 3)
		first_bit = 3;
	((HashJoin*)root)->Configure(rb1,rb2,first_bit, NULL, NULL, NULL, NULL);
	}
	//root->Configure(4,4,4, NULL, NULL, NULL, NULL);
	

	int case_statement;
	for(size_t i=1; i != qinfo.predicates.size(); i++){

		PredicateInfo& pinfo = qinfo.predicates[i];
		SelectInfo& left_info = pinfo.left;
		SelectInfo& right_info = pinfo.right;
		Operator *left,*right,*center;

		case_statement = analyze_input_join(used_rel,left_info,right_info);
		if(case_statement == 0){
			//If the left relation has already been used before then
			//std::cerr<<"Left Join....."<<std::endl;
			left = move(root);
			right = _addScan(used_rel,right_info,qinfo);
			
			//find_needed(qinfo,right_info,return_needed);

			for (unsigned i = 0; i < right->GetColumnCount(); i++)
				return_needed.push_back(i);

			//push_proj_r = new Projection(move(right),return_needed);


			//push_proj_r->Open();

			cright = return_needed.size();


			right_proj_colId = vector_index(return_needed,right_info.colId);

			push_fil_r = _addFilter(right_info.binding,right,qinfo);

			//std::cout<<"Hash on left:"<<mapper[left_info.relId][left_info.colId]<<"Hash on right:"<<right_info.colId<<std::endl;
			root = new HashJoin(std::move(left),left->GetColumnCount(),mapper[left_info.binding][left_info.colId],std::move(push_fil_r),cright,right_proj_colId,1 << 10);
			column_update(mapper,cright);
			mapper_insert(mapper,right_info.binding,return_needed,0);
			return_needed.clear();
			{
			int numrows = catalog_[right_info.relId][0];
			int maxval = catalog_[right_info.relId][2+(right_info.colId*3)+2];

			int partsize = 1 << 10;
			int numparts = numrows / partsize;
			if (numparts < 128)
				numparts = 128;
			int rbits = ((int) (log2(numparts))) + 1;
			int rb1 = rbits/2;
			int rb2 = rbits-rb1;
			int first_bit = ((int) log2(maxval)) - rbits;
			if (first_bit < 3)
				first_bit = 3;
			((HashJoin*)root)->Configure(rb1,rb2,first_bit, NULL, NULL, NULL, NULL);
			}

			//root->Configure(4,4,4, NULL, NULL, NULL, NULL);
			//root->Open();
		}
		else if(case_statement == 1){
			//The right relation has been used
			//std::cout<<"RIGHT"<<std::endl;

			//std::cerr<<"Right Join....."<<std::endl;
			left = _addScan(used_rel,left_info,qinfo);
			right = move(root);

			//find_needed(qinfo,left_info,return_needed);

			for (unsigned i = 0; i < left->GetColumnCount(); i++)
				return_needed.push_back(i);

			//push_proj_l = new Projection(move(left),return_needed);
			//push_proj_l->Open();
			push_fil_l = _addFilter(left_info.binding,left,qinfo);

			mapper_insert(mapper,left_info.binding,return_needed,right->GetColumnCount());


			cleft = return_needed.size();
			left_proj_colId = vector_index(return_needed,left_info.colId);
			
			root = new HashJoin(std::move(push_fil_l),cleft,left_proj_colId,std::move(right),right->GetColumnCount(),mapper[right_info.binding][right_info.colId],1 << 10);
			return_needed.clear();
			{
			int numrows = catalog_[left_info.relId][0];
			int maxval = catalog_[left_info.relId][2+(left_info.colId*3)+2];

			int partsize = 1 << 10;
			int numparts = numrows / partsize;
			if (numparts < 128)
				numparts = 128;
			int rbits = ((int) (log2(numparts))) + 1;
			int rb1 = rbits/2;
			int rb2 = rbits-rb1;
			int first_bit = ((int) log2(maxval)) - rbits;
			if (first_bit < 3)
				first_bit = 3;
			((HashJoin*)root)->Configure(rb1,rb2,first_bit, NULL, NULL, NULL, NULL);
			}
			//root->Configure(4,4,4, NULL, NULL, NULL, NULL);
			//root->Open();
		}
		else if(case_statement == 2){
			//All relations of this join are aready used 
			//std::cerr<<"Self Join....."<<std::endl;
			center = move(root);
			root = new SelfJoin(move(center),mapper[left_info.binding][left_info.colId],mapper[right_info.binding][right_info.colId],center->GetColumnCount());
			//root->Open(); 
		}
		else{
			//We havent seen yet both of these relations so delay this so we may connect it to 
			//other join
			qinfo.predicates.push_back(pinfo);
		}
	}
	
	std::vector<unsigned> proj_mapper;
	for(auto& proj: qinfo.selections){
		proj_mapper.push_back(mapper[proj.binding][proj.colId]);
		//std::cout<<"Columns "<<mapper[proj.relId][proj.colId]<<" ";
	}
	//std::cerr<<"Projection....."<<std::endl;
	Projection* proj = new Projection(root,proj_mapper);
	//proj->Open();
	//std::cerr<<"Checksum....."<<std::endl;
	CheckSum checkSum(proj);
	
	//checkSum.Print();

	std::map<std::pair<unsigned, unsigned>, unsigned> bd;
	//std::cerr<<"ProjectionPass....."<<std::endl;
	Operator* newroot = checkSum.ProjectionPass(bd);

	//MaterInfo info(this->relations, qinfo.relationIds);
    //Operator *newroot = checkSum.LateMaterialize(std::move(info));

	//newroot->Print();

	std::vector<uint64_t*> results;

	//std::cerr<<"NewRoot Open...."<<std::endl;

	newroot->Open();

	//std::cerr<<"NewRoot Next....."<<std::endl;

	newroot->Next(&results);

	
	/*checkSum.Open();
	checkSum.Next(&results);*/

	//checkSum.Next(&results);
	//std::cerr<<"Results....."<<std::endl;

	stringstream out;
	for(unsigned i=0;i<qinfo.results.size();++i) {
		if (results[qinfo.results[i]])
			out << to_string((uint64_t)results[qinfo.results[i]]);
		else
			out << "NULL";
		if (i != qinfo.results.size()-1)
			out << " ";
	}
	newroot->Close();
	//checkSum.Close();
	//cache.clear();
	//return new QueryJob(newroot, wr_offset, result_buffer, qinfo);
	//We have to close all the opened operators
	return out.str();


}
